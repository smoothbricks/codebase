//! The kqueue arm of the shell-input subscription: one `EVFILT_VNODE` filter per watched vnode.

use std::collections::BTreeSet;
use std::ffi::CString;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;

use super::Target;

/// One kqueue with an `EVFILT_VNODE` filter per watched vnode.
pub struct Subscription {
    queue: OwnedFd,
    /// Held open for the subscription's lifetime: closing a vnode's descriptor removes its
    /// filter. `O_EVTONLY` never keeps a volume from unmounting.
    _vnodes: Vec<OwnedFd>,
}

const EVENTS: u32 = libc::NOTE_DELETE
    | libc::NOTE_WRITE
    | libc::NOTE_EXTEND
    | libc::NOTE_ATTRIB
    | libc::NOTE_LINK
    | libc::NOTE_RENAME
    | libc::NOTE_REVOKE;

impl Subscription {
    pub fn new(targets: &BTreeSet<(PathBuf, Target)>) -> io::Result<Self> {
        // SAFETY: kqueue takes no arguments and returns a new descriptor or -1.
        let raw = unsafe { libc::kqueue() };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` is a freshly created descriptor this function owns.
        let queue = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: plain fcntl on an owned descriptor.
        if unsafe { libc::fcntl(queue.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut vnodes = Vec::with_capacity(targets.len());
        let mut changes = Vec::with_capacity(targets.len());
        for (path, _) in targets {
            let path = CString::new(path.as_os_str().as_bytes())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            // SAFETY: `path` is a NUL-terminated string that outlives the call.
            let raw = unsafe { libc::open(path.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
            if raw < 0 {
                // Vanished between the stat and the open: that is itself a change, which
                // the pool's stat backstop reports on the next decision.
                continue;
            }
            // SAFETY: `raw` is a freshly opened descriptor this function owns.
            let vnode = unsafe { OwnedFd::from_raw_fd(raw) };
            changes.push(libc::kevent {
                ident: usize::try_from(vnode.as_raw_fd()).map_err(io::Error::other)?,
                filter: libc::EVFILT_VNODE,
                flags: libc::EV_ADD | libc::EV_CLEAR,
                fflags: EVENTS,
                data: 0,
                udata: std::ptr::null_mut(),
            });
            vnodes.push(vnode);
        }
        if !changes.is_empty() {
            let count = i32::try_from(changes.len()).map_err(io::Error::other)?;
            // SAFETY: `changes` holds `count` initialized kevents; no event list is read.
            let registered = unsafe {
                libc::kevent(
                    queue.as_raw_fd(),
                    changes.as_ptr(),
                    count,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            };
            if registered < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(Self {
            queue,
            _vnodes: vnodes,
        })
    }

    pub fn drain(&self) -> io::Result<bool> {
        let mut changed = false;
        let immediately = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let mut events = [libc::kevent {
            ident: 0,
            filter: 0,
            flags: 0,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        }; 64];
        loop {
            // SAFETY: `events` has room for 64 kevents and the zero timeout never blocks.
            let received = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    events.as_mut_ptr(),
                    64,
                    &immediately,
                )
            };
            if received < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }
            if received > 0 {
                changed = true;
            }
            if received < 64 {
                return Ok(changed);
            }
        }
    }
}
