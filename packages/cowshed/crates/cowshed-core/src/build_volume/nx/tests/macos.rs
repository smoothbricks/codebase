//! The Nx tests' event queue on macOS: one kqueue of vnode and process filters.

use std::io;
use std::path::Path;

use super::Event;

/// One kernel event queue: a file's write, extend or attribute change, and processes' exits.
pub(super) struct Events {
    queue: std::os::fd::OwnedFd,
    _watched: Vec<std::os::fd::OwnedFd>,
}

impl Events {
    pub(super) fn new() -> Self {
        use std::os::fd::FromRawFd as _;
        // SAFETY: `kqueue` takes no arguments; a negative answer is an error.
        let queue = unsafe { libc::kqueue() };
        assert!(queue >= 0, "kqueue: {}", io::Error::last_os_error());
        Self {
            // SAFETY: `queue` is a fresh descriptor only this value owns.
            queue: unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) },
            _watched: Vec::new(),
        }
    }

    fn add(&self, ident: libc::uintptr_t, filter: i16, fflags: u32) {
        use std::os::fd::AsRawFd as _;
        let change = libc::kevent {
            ident,
            filter,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: one valid change, no events requested back.
        let added = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        assert!(added == 0, "kevent: {}", io::Error::last_os_error());
    }

    /// Changes to `path`'s contents or attributes, watched without holding it open.
    pub(super) fn file(&mut self, path: &Path) {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        use std::os::unix::ffi::OsStrExt as _;
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `name` is NUL-terminated; a negative answer is an error.
        let file = unsafe { libc::open(name.as_ptr(), libc::O_EVTONLY) };
        assert!(
            file >= 0,
            "open {}: {}",
            path.display(),
            io::Error::last_os_error()
        );
        // SAFETY: `file` is a fresh descriptor only this value owns.
        let file = unsafe { std::os::fd::OwnedFd::from_raw_fd(file) };
        self.add(
            file.as_raw_fd() as libc::uintptr_t,
            libc::EVFILT_VNODE,
            libc::NOTE_WRITE | libc::NOTE_EXTEND | libc::NOTE_ATTRIB,
        );
        self._watched.push(file);
    }

    pub(super) fn exit(&self, pid: u32) {
        self.add(pid as libc::uintptr_t, libc::EVFILT_PROC, libc::NOTE_EXIT);
    }

    /// The next event, waiting for it.
    pub(super) fn next(&self) -> Event {
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
                // SAFETY: the kernel wrote one event.
                return match unsafe { event.assume_init() }.filter {
                    libc::EVFILT_VNODE => Event::File,
                    libc::EVFILT_PROC => Event::Exit,
                    filter => panic!("kevent answered filter {filter}, which nothing registered"),
                };
            }
            let error = io::Error::last_os_error();
            assert_eq!(error.kind(), io::ErrorKind::Interrupted, "kevent: {error}");
        }
    }
}
