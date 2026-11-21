//! What Linux shows of a held lock: each descriptor's lock in `/proc/<pid>/fdinfo/<fd>`, and the
//! kernel's lock table in `/proc/locks`.

use std::fs::{self, File};
use std::io;

use super::{Inode, KernelView, Seen};
use crate::lease_holder;

/// Linux: each process with a descriptor on `inode` and whether its fdinfo shows the flock, and
/// whether the kernel's lock table shows the inode locked (`/proc/locks`, which also covers a
/// holder of another user whose descriptors this user cannot read).
pub(super) fn kernel_view(
    census: &crate::census::CensusScope,
    _file: &File,
    inode: Inode,
    _recorded: Option<&lease_holder::Process>,
) -> io::Result<(bool, Vec<Seen>)> {
    let mut seen = Vec::new();
    for (holder, fds) in
        crate::build_volume::nx::inode_holders(census, (inode.dev.get(), inode.ino.get()))?
    {
        let view = fdinfo_custody(
            fds.iter()
                .map(|fd| fs::read_to_string(format!("/proc/{}/fdinfo/{fd}", holder.pid))),
            u64::from(libc::major(inode.dev.get())),
            u64::from(libc::minor(inode.dev.get())),
            inode.ino.get(),
        );
        if let Some(view) = view {
            seen.push(Seen {
                pid: holder.pid,
                command: holder.command,
                view,
            });
        }
    }
    let locked = seen
        .iter()
        .any(|process| process.view == KernelView::Custody { holds: true })
        || lock_table_holds(&fs::read_to_string("/proc/locks")?, inode);
    Ok((locked, seen))
}

/// Aggregate descriptor evidence without turning an unreadable candidate into negative custody.
/// A proven grant wins; otherwise any unreadable candidate is unexposed. Disappeared candidates
/// contribute nothing, and an entirely disappeared process is omitted from the observation.
fn fdinfo_custody<S: AsRef<str>>(
    infos: impl IntoIterator<Item = io::Result<S>>,
    major: u64,
    minor: u64,
    ino: u64,
) -> Option<KernelView> {
    let mut view = None;
    for info in infos {
        match info {
            Ok(info) => {
                if fdinfo_holds_flock(info.as_ref(), major, minor, ino) {
                    return Some(KernelView::Custody { holds: true });
                }
                view.get_or_insert(KernelView::Custody { holds: false });
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => view = Some(KernelView::Unexposed),
        }
    }
    view
}

/// Whether fdinfo names a granted `flock(2)` on the requested inode, not a different lock on a
/// reused descriptor. Waiters are never granted custody; `/proc/locks` marks them `->`.
fn fdinfo_holds_flock(info: &str, major: u64, minor: u64, ino: u64) -> bool {
    info.lines().any(|line| {
        line.strip_prefix("lock:")
            .is_some_and(|lock| lock_line_holds(lock, major, minor, ino))
    })
}

/// Whether `/proc/locks` shows a granted `flock(2)` on `inode`: a line `N: FLOCK ... <pid>
/// <maj>:<min>:<ino> ...` (hexadecimal major and minor) that is no `->` waiter.
fn lock_table_holds(table: &str, inode: Inode) -> bool {
    let (dev, ino) = (inode.dev.get(), inode.ino.get());
    let (major, minor) = (libc::major(dev), libc::minor(dev));
    table
        .lines()
        .any(|line| lock_line_holds(line, u64::from(major), u64::from(minor), ino))
}

fn lock_line_holds(line: &str, major: u64, minor: u64, ino: u64) -> bool {
    let mut fields = line.split_whitespace();
    fields.next();
    if fields.next() != Some("FLOCK") {
        return false;
    }
    fields.any(|field| {
        let mut parts = field.split(':');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(maj), Some(min), Some(number), None) => {
                u64::from_str_radix(maj, 16).ok() == Some(major)
                    && u64::from_str_radix(min, 16).ok() == Some(minor)
                    && number.parse::<u64>().ok() == Some(ino)
            }
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_fdinfo_lock_line_is_a_granted_flock_and_nothing_else_is() {
        let held = "pos:\t0\nflags:\t02100002\nmnt_id:\t31\nino:\t1311\nlock:\t1: FLOCK  ADVISORY  WRITE 4242 08:02:1311 0 EOF\n";
        assert!(fdinfo_holds_flock(held, 8, 2, 1311));
        assert!(!fdinfo_holds_flock(held, 8, 2, 1312));
        assert!(!fdinfo_holds_flock(held, 9, 2, 1311));
        let posix = "pos:\t0\nlock:\t1: POSIX  ADVISORY  WRITE 4242 08:02:1311 0 EOF\n";
        assert!(!fdinfo_holds_flock(posix, 8, 2, 1311));
        assert!(!fdinfo_holds_flock(
            "pos:\t0\nflags:\t02100002\n",
            8,
            2,
            1311
        ));
    }

    #[test]
    fn unreadable_fdinfo_is_unexposed_and_disappeared_fdinfo_is_not_a_descriptor() {
        let held = "lock:\t1: FLOCK ADVISORY WRITE 4242 08:02:1311 0 EOF\n";
        let open = "pos:\t0\nflags:\t02100002\n";
        let gone = || Err::<&str, _>(io::Error::from(io::ErrorKind::NotFound));
        let unreadable = || Err::<&str, _>(io::Error::from(io::ErrorKind::PermissionDenied));
        assert_eq!(fdinfo_custody([gone()], 8, 2, 1311), None);
        assert_eq!(
            fdinfo_custody([unreadable()], 8, 2, 1311),
            Some(KernelView::Unexposed)
        );
        assert_eq!(
            fdinfo_custody([Ok(open), gone()], 8, 2, 1311),
            Some(KernelView::Custody { holds: false })
        );
        for infos in [[Ok(open), unreadable()], [unreadable(), Ok(open)]] {
            assert_eq!(
                fdinfo_custody(infos, 8, 2, 1311),
                Some(KernelView::Unexposed)
            );
        }
        for infos in [[Ok(held), unreadable()], [unreadable(), Ok(held)]] {
            assert_eq!(
                fdinfo_custody(infos, 8, 2, 1311),
                Some(KernelView::Custody { holds: true })
            );
        }
        assert_eq!(
            fdinfo_custody([Ok(held), unreadable()], 8, 2, 1312),
            Some(KernelView::Unexposed)
        );
    }

    #[test]
    fn a_lock_table_line_names_its_inode_and_never_a_waiter() {
        let granted = "1: FLOCK  ADVISORY  WRITE 4242 fd:01:1311 0 EOF";
        assert!(lock_line_holds(granted, 0xfd, 1, 1311));
        assert!(!lock_line_holds(granted, 0xfd, 1, 1312));
        assert!(!lock_line_holds(granted, 8, 1, 1311));
        let waiter = "1: -> FLOCK  ADVISORY  WRITE 4343 fd:01:1311 0 EOF";
        assert!(!lock_line_holds(waiter, 0xfd, 1, 1311));
    }
}
