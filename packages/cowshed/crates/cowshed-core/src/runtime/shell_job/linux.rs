//! What a warm-shell job takes from Linux: a pipe pair `pipe2` makes close-on-exec at birth, and a
//! command's start handed to its host with the job's cgroup placement (`shell_host.rs`, "Wire
//! protocol"). The host's forked command enters the cgroup before it executes or interprets
//! anything, with no hold: this process attaches the job's observer once the host reports the
//! command's birth, while the host still holds the leader unreaped.

use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};

use crate::api::process::ProcessCoverageGap;
use crate::error::Result;
use crate::runtime::job_groups::Birth;
use crate::runtime::process_monitor::ProcessMonitor;
use crate::runtime::supervisor::CommandPlacement;

/// [`super::pipe`], running `between` once the pair exists and before it is handed out. `pipe2`
/// makes the pair close-on-exec in the one system call, so no spawn can come between.
pub(super) fn pipe_with(between: impl FnOnce()) -> io::Result<(OwnedFd, OwnedFd)> {
    let mut ends = [0; 2];
    // SAFETY: `ends` has room for the two descriptors pipe2(2) writes.
    if unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: both descriptors were just created and are owned here alone.
    let ends = unsafe { (OwnedFd::from_raw_fd(ends[0]), OwnedFd::from_raw_fd(ends[1])) };
    between();
    Ok(ends)
}

/// A command's start as it is handed to its host: a write end of the job cgroup's
/// `cgroup.procs`, which travels with the request after the job's stdio.
pub(super) struct Handoff {
    placement: OwnedFd,
}

impl Handoff {
    pub(super) fn prepare(placement: &CommandPlacement) -> Result<Self> {
        Ok(Self {
            placement: placement.descriptor()?,
        })
    }

    /// The descriptors the request carries: the job's stdio, then the cgroup placement.
    pub(super) fn descriptors(&self, stdio: [RawFd; 3]) -> [RawFd; 4] {
        [stdio[0], stdio[1], stdio[2], self.placement.as_raw_fd()]
    }

    /// The request is sent: the host holds its own copy of the placement, so this one closes.
    /// The cgroup the command enters carries its class, so nothing is placed here.
    pub(super) async fn sent(
        self,
        _host_pid: i32,
        _placement: &CommandPlacement,
        _monitor: &ProcessMonitor,
    ) -> Result<Placed> {
        drop(self.placement);
        Ok(Placed)
    }
}

/// A command handed to its host with its cgroup placement. No fence holds it and no nice is
/// placed on it.
pub(super) struct Placed;

impl Placed {
    /// The host says a command started. No fence was to hold it: it entered its cgroup itself.
    pub(super) fn fenced(&self) -> Result<()> {
        Ok(())
    }

    /// Attach `monitor` to the started command `birth` names, which its host holds unreaped. The
    /// command already runs, so an observer that cannot attach does not fail it: the job's tree
    /// then states the birth it never observed, and the job's cgroup still holds every member.
    /// No nice was placed, so none is observed.
    pub(super) async fn started(self, birth: &Birth, monitor: &ProcessMonitor) -> Option<i32> {
        let pid = birth.pid();
        let monitor = monitor.clone();
        let unobserved = tokio::task::spawn_blocking(move || {
            let refused = monitor.watch(pid).err()?;
            let gap = monitor
                .gap(ProcessCoverageGap::UnobservedBirth { pid })
                .err();
            Some((refused, gap))
        })
        .await
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic.into_panic()));
        if let Some((refused, gap)) = unobserved {
            cowshed_os::eprintln!(
                "cowshed: the process observer did not attach to the started command {pid}: {}",
                refused.message
            );
            if let Some(gap) = gap {
                cowshed_os::eprintln!(
                    "cowshed: the unobserved birth of command {pid} could not be recorded: {}",
                    gap.message
                );
            }
        }
        None
    }
}

/// Clone the bound program `source` to a new file at `destination` (`FICLONE`, which reads no
/// descriptor offset); `false`, with nothing left at `destination`, when the two are on
/// different filesystems or theirs cannot clone.
pub(super) fn clone_bound(
    source: &std::fs::File,
    destination: &std::path::Path,
) -> io::Result<bool> {
    let made = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    // The request argument of `ioctl` is `c_ulong` under glibc and `c_int` under musl.
    #[allow(clippy::unnecessary_cast)]
    let request = libc::FICLONE as libc::Ioctl;
    // SAFETY: both descriptors stay open across the call, which takes the source's by value.
    if unsafe { libc::ioctl(made.as_raw_fd(), request, source.as_raw_fd()) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    std::fs::remove_file(destination)?;
    match error.raw_os_error() {
        Some(libc::EXDEV | libc::EOPNOTSUPP | libc::EINVAL | libc::ENOTTY) => Ok(false),
        _ => Err(error),
    }
}
