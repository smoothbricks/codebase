//! The discard directory's watch on macOS: a kqueue vnode filter on the open directory.

use std::fs;
use std::io;
use std::path::Path;

/// One blocking native watch on a checkout's discard directory: a leaf's removal is a
/// directory write there. Armed before the first read it waits on, so no removal that
/// the read could miss is lost between the read and the wait.
pub(super) struct DiscardWatch {
    queue: std::os::fd::OwnedFd,
    _directory: fs::File,
}

impl DiscardWatch {
    pub(super) fn arm(directory: &Path) -> Option<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        let directory = match fs::File::open(directory) {
            Ok(directory) => directory,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => panic!("open the discard directory: {error}"),
        };
        // SAFETY: `kqueue` takes no arguments; a negative answer is an error.
        let queue = unsafe { libc::kqueue() };
        assert!(queue >= 0, "kqueue: {}", io::Error::last_os_error());
        // SAFETY: `queue` is a fresh descriptor only this value owns.
        let queue = unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) };
        let change = libc::kevent {
            ident: usize::try_from(directory.as_raw_fd()).expect("a descriptor"),
            filter: libc::EVFILT_VNODE,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: libc::NOTE_WRITE,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: one valid change, no events requested back.
        let added = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        assert!(added == 0, "kevent: {}", io::Error::last_os_error());
        Some(Self {
            queue,
            _directory: directory,
        })
    }

    pub(super) fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let mut event = std::mem::MaybeUninit::<libc::kevent>::zeroed();
        loop {
            // SAFETY: room for one event; no timeout.
            let ready = unsafe {
                libc::kevent(
                    self.queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    event.as_mut_ptr(),
                    1,
                    std::ptr::null(),
                )
            };
            if ready == 1 {
                return;
            }
            let error = io::Error::last_os_error();
            assert_eq!(error.kind(), io::ErrorKind::Interrupted, "kevent: {error}");
        }
    }
}
