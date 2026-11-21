//! The discard directory's watch on Linux: an inotify watch for entries leaving or entering it.

use std::io;
use std::path::Path;

/// One blocking native watch on a checkout's discard directory: a leaf's removal is a
/// directory event there. Armed before the first read it waits on, so no removal that
/// the read could miss is lost between the read and the wait.
pub(super) struct DiscardWatch {
    inotify: std::os::fd::OwnedFd,
}

impl DiscardWatch {
    pub(super) fn arm(directory: &Path) -> Option<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::os::unix::ffi::OsStrExt as _;
        if !directory.exists() {
            return None;
        }
        // SAFETY: inotify_init1 takes flags only; a negative answer is an error.
        let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
        assert!(raw >= 0, "inotify_init1: {}", io::Error::last_os_error());
        // SAFETY: `raw` is a fresh descriptor only this value owns.
        let inotify = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };
        let path =
            std::ffi::CString::new(directory.as_os_str().as_bytes()).expect("a directory path");
        // SAFETY: `path` is NUL-terminated and outlives the call.
        let watch = unsafe {
            libc::inotify_add_watch(
                inotify.as_raw_fd(),
                path.as_ptr(),
                libc::IN_DELETE | libc::IN_MOVED_FROM | libc::IN_CREATE | libc::IN_MOVED_TO,
            )
        };
        assert!(
            watch >= 0,
            "inotify_add_watch {}: {}",
            directory.display(),
            io::Error::last_os_error()
        );
        Some(Self { inotify })
    }

    pub(super) fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let mut buffer = [0_u8; 4096];
        loop {
            // SAFETY: `buffer` is writable for its full length; the descriptor is owned
            // and blocking, so the read returns only with events or an error.
            let read = unsafe {
                libc::read(
                    self.inotify.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            if read > 0 {
                return;
            }
            let error = io::Error::last_os_error();
            assert_eq!(
                error.kind(),
                io::ErrorKind::Interrupted,
                "inotify read: {error}"
            );
        }
    }
}
