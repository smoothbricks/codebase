//! The inotify arm of the shell-input subscription: one watch per target on one instance.

use std::collections::BTreeSet;
use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;

use super::Target;

/// What reports a change of an input itself. A regular file answers to the first five; the
/// entry events fire only when the input is a directory, where they are what kqueue's
/// `NOTE_WRITE` reports. A directory watch also reports its children's writes and attribute
/// changes, which costs the pool one recertification and nothing else.
const INPUT: u32 = libc::IN_MODIFY
    | libc::IN_ATTRIB
    | libc::IN_CLOSE_WRITE
    | libc::IN_MOVE_SELF
    | libc::IN_DELETE_SELF
    | libc::IN_CREATE
    | libc::IN_DELETE
    | libc::IN_MOVED_FROM
    | libc::IN_MOVED_TO;

/// What reports a missing input appearing under its nearest existing ancestor: an entry created
/// or moved in, or the ancestor itself moved or removed, after which the path resolves
/// elsewhere.
const ANCESTOR: u32 =
    libc::IN_CREATE | libc::IN_MOVED_TO | libc::IN_MOVE_SELF | libc::IN_DELETE_SELF;

/// One non-blocking inotify instance with a watch per target.
pub struct Subscription {
    inotify: OwnedFd,
}

impl Subscription {
    pub fn new(targets: &BTreeSet<(PathBuf, Target)>) -> io::Result<Self> {
        // SAFETY: inotify_init1 takes flags only and returns a new descriptor or -1.
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` is a freshly created descriptor this function owns.
        let inotify = unsafe { OwnedFd::from_raw_fd(raw) };
        for (path, target) in targets {
            // One inode reached as two targets (an input directory that is also a missing
            // input's ancestor, or two links to one file) keeps the union of their masks:
            // without `IN_MASK_ADD` the later watch replaces the earlier one's.
            let mask = libc::IN_MASK_ADD
                | match target {
                    Target::File => INPUT,
                    Target::Directory => ANCESTOR,
                };
            let path = CString::new(path.as_os_str().as_bytes())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            // SAFETY: `path` is NUL-terminated and outlives the call.
            // A path that vanished since the stat is a change the stat backstop reports.
            let _ = unsafe { libc::inotify_add_watch(inotify.as_raw_fd(), path.as_ptr(), mask) };
        }
        Ok(Self { inotify })
    }

    pub fn drain(&self) -> io::Result<bool> {
        let mut changed = false;
        let mut buffer = [0_u8; 4096];
        loop {
            // SAFETY: `buffer` is writable for its full length; the descriptor is owned.
            let read = unsafe {
                libc::read(
                    self.inotify.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            if read > 0 {
                changed = true;
                continue;
            }
            if read == 0 {
                return Ok(changed);
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::WouldBlock => return Ok(changed),
                io::ErrorKind::Interrupted => {}
                _ => return Err(error),
            }
        }
    }
}
