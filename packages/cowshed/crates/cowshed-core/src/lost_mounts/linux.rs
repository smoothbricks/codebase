//! Linux's mount table and its change events, both `/proc/self/mountinfo`
//! (proc_pid_mountinfo(5)).
//!
//! The table is the file's text, one mount a line. The event is the file's poll readiness: the
//! kernel raises `POLLPRI | POLLERR` on an open `mountinfo` whenever a mount or unmount changed
//! the namespace since that descriptor's last poll, whoever made the change. The event names no
//! mount: a reader learns which went by reading the table again.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs::{self, File};
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::ffi::OsStringExt as _;
use std::path::PathBuf;

const MOUNTINFO: &str = "/proc/self/mountinfo";

/// One mount of this process's namespace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Mount {
    /// The kernel's id of the mount, the one `statx` answers as `stx_mnt_id`.
    pub(crate) id: u64,
    pub(crate) mount_point: PathBuf,
}

/// Every mount the kernel holds now, in the kernel's order.
pub(crate) fn mounts() -> io::Result<Vec<Mount>> {
    parse(&fs::read_to_string(MOUNTINFO)?)
}

/// Every mount of `table`. A line that does not parse fails the whole read: a table with a hole
/// in it cannot say that a mount went.
fn parse(table: &str) -> io::Result<Vec<Mount>> {
    table
        .lines()
        .map(|line| {
            // `<id> <parent> <major:minor> <root> <mount point> …`
            let mut fields = line.split(' ');
            let id = fields.next().and_then(|id| id.parse::<u64>().ok());
            match (id, fields.nth(3)) {
                (Some(id), Some(mount_point)) => Ok(Mount {
                    id,
                    mount_point: PathBuf::from(OsString::from_vec(unescape(mount_point))),
                }),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("malformed {MOUNTINFO} line {line:?}"),
                )),
            }
        })
        .collect()
}

/// `mountinfo` writes space, tab, newline and backslash as `\ooo`.
fn unescape(field: &str) -> Vec<u8> {
    let bytes = field.as_bytes();
    let mut plain = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    while let Some((&byte, after)) = rest.split_first() {
        let octal = after
            .get(..3)
            .filter(|_| byte == b'\\')
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        match octal {
            Some(escaped) => {
                plain.push(escaped);
                rest = &after[3..];
            }
            None => {
                plain.push(byte);
                rest = after;
            }
        }
    }
    plain
}

/// The mount points the kernel holds now: what two reads are compared by.
pub(super) fn kernel_mounts() -> io::Result<BTreeSet<PathBuf>> {
    Ok(mounts()?
        .into_iter()
        .map(|mount| mount.mount_point)
        .collect())
}

/// The mount points `before` held that `after` holds no longer: what went between two reads of
/// the kernel's table, by the path the kernel reports.
pub(super) fn vanished(before: &BTreeSet<PathBuf>, after: &BTreeSet<PathBuf>) -> BTreeSet<PathBuf> {
    before.difference(after).cloned().collect()
}

/// The kernel's word that this namespace's mount table changed: an open `mountinfo`, whose poll
/// raises `POLLPRI | POLLERR` once per change of the table since the descriptor's last poll.
#[derive(Debug)]
pub(super) struct MountTableEvents {
    table: File,
}

impl MountTableEvents {
    /// Open the table to poll. A change made after this returns is never missed: the kernel
    /// records the table's event count at the open and compares against it on every poll.
    pub(super) fn new() -> io::Result<Self> {
        Ok(Self {
            table: File::open(MOUNTINFO)?,
        })
    }

    /// Block until the kernel signals a change of the mount table, then answer. Every change
    /// made before the call is folded into the one event it takes, so a burst of unmounts wakes
    /// a reader once.
    pub(super) fn next(&self) -> io::Result<()> {
        // `mountinfo` is always readable, so only the exceptional conditions are asked for:
        // `POLLERR` is reported whether requested or not.
        let mut poll = libc::pollfd {
            fd: self.table.as_raw_fd(),
            events: libc::POLLPRI,
            revents: 0,
        };
        loop {
            // SAFETY: one live pollfd entry naming the descriptor this value owns; no timeout.
            let ready = unsafe { libc::poll(&raw mut poll, 1, -1) };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            } else if poll.revents & libc::POLLNVAL != 0 {
                return Err(io::Error::other(format!(
                    "{MOUNTINFO} is no longer open to poll"
                )));
            } else if poll.revents & (libc::POLLPRI | libc::POLLERR) != 0 {
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mountinfo_lines_parse_with_their_escapes_and_a_malformed_line_fails_the_read() {
        let table = "36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue\n\
                     91 36 0:44 / /srv/with\\040space\\134and\\011tab rw shared:5 - zfs tank/a\\040b rw\n";
        assert_eq!(
            parse(table).unwrap(),
            [
                Mount {
                    id: 36,
                    mount_point: PathBuf::from("/mnt2"),
                },
                Mount {
                    id: 91,
                    mount_point: PathBuf::from("/srv/with space\\and\ttab"),
                },
            ]
        );
        assert_eq!(
            parse("36 35 98:0 /mnt1\n").unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            parse("first 35 98:0 / /mnt rw - ext4 /dev/sda rw\n")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn vanished_names_only_the_mount_points_that_went() {
        let before = BTreeSet::from([PathBuf::from("/a"), PathBuf::from("/b")]);
        let after = BTreeSet::from([PathBuf::from("/b"), PathBuf::from("/c")]);
        assert_eq!(
            vanished(&before, &after),
            BTreeSet::from([PathBuf::from("/a")])
        );
    }

    #[test]
    fn the_live_table_holds_the_root_mount() {
        assert!(
            mounts()
                .unwrap()
                .iter()
                .any(|mount| mount.mount_point == std::path::Path::new("/"))
        );
    }
}
