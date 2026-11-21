//! What Darwin shows of a held lock: which processes have a descriptor on the inode and each
//! one's kernel unique id, and through `F_GETLK` whether the inode is locked at all.

use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd as _;

use super::{Inode, KernelView, Seen};
use crate::lease_holder;
use crate::runtime::job_groups;

/// Darwin: each process with a descriptor on `inode` and its kernel unique id, and whether
/// `F_GETLK` shows the inode exclusively locked: a `flock(2)` conflicts with the shared lock it
/// asks about. The descriptor census asks no process a cowshed lifecycle operation of `census`'s
/// store runs in ([`crate::build_volume::nx::inode_holders`]): a fork's first mount of its new image
/// holds its vnode for minutes. The lock's `recorded` holder, when it is such a process, is seen by
/// its unique id alone, which the process table answers without reading a descriptor, so a fork
/// holding `main.dmg.lock` is still its proven owner.
pub(super) fn kernel_view(
    census: &crate::census::CensusScope,
    file: &File,
    inode: Inode,
    recorded: Option<&lease_holder::Process>,
) -> io::Result<(bool, Vec<Seen>)> {
    let mut seen = Vec::new();
    for (holder, _fds) in
        crate::build_volume::nx::inode_holders(census, (inode.dev.get(), inode.ino.get()))?
    {
        let unique_id = job_groups::process_record(holder.pid)?.map(|record| record.unique_id);
        seen.push(Seen {
            pid: holder.pid,
            command: holder.command,
            view: KernelView::Identity { unique_id },
        });
    }
    if let Some(recorded) = recorded
        && let Ok(pid) = i32::try_from(recorded.pid)
        && !seen.iter().any(|process| process.pid == pid)
        && lease_holder::running_lifecycle(census.store(), |lease| fs::read(lease)).contains(&pid)
        && let Some(record) = job_groups::process_record(pid)?
    {
        seen.push(Seen {
            pid,
            command: recorded.command.clone(),
            view: KernelView::Identity {
                unique_id: Some(record.unique_id),
            },
        });
    }
    // SAFETY: an all-zero `flock` is a valid value of the plain C struct.
    let mut probe = unsafe { std::mem::zeroed::<libc::flock>() };
    probe.l_type = libc::c_short::try_from(libc::F_RDLCK).map_err(io::Error::other)?;
    probe.l_whence = libc::c_short::try_from(libc::SEEK_SET).map_err(io::Error::other)?;
    // SAFETY: F_GETLK reads and writes `probe`, a live `flock` this function owns.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETLK, &mut probe) } == -1 {
        return Err(io::Error::last_os_error());
    }
    let locked =
        probe.l_type != libc::c_short::try_from(libc::F_UNLCK).map_err(io::Error::other)?;
    Ok((locked, seen))
}
