//! The Nx tests' event queue on Linux: one inotify descriptor and a pidfd per process, polled
//! together.

use std::cell::RefCell;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::path::Path;

use cowshed_os::process::ExitWatch;

use super::Event;

/// One kernel event queue: a file's write, extend or attribute change, and processes' exits.
pub(super) struct Events {
    inotify: OwnedFd,
    /// The processes whose exits are not yet reported. An exited process's pidfd stays readable,
    /// so each leaves once it is reported: an exit is one event.
    exits: RefCell<Vec<ExitWatch>>,
}

impl Events {
    pub(super) fn new() -> Self {
        // SAFETY: inotify_init1 takes flags only; a negative answer is an error.
        let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
        assert!(raw >= 0, "inotify_init1: {}", io::Error::last_os_error());
        Self {
            // SAFETY: `raw` is a fresh descriptor only this value owns.
            inotify: unsafe { OwnedFd::from_raw_fd(raw) },
            exits: RefCell::new(Vec::new()),
        }
    }

    /// Changes to `path`'s contents or attributes, watched without holding it open.
    pub(super) fn file(&mut self, path: &Path) {
        use std::os::unix::ffi::OsStrExt as _;
        let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: `name` is NUL-terminated and outlives the call.
        let watch = unsafe {
            libc::inotify_add_watch(
                self.inotify.as_raw_fd(),
                name.as_ptr(),
                libc::IN_MODIFY | libc::IN_ATTRIB,
            )
        };
        assert!(
            watch >= 0,
            "inotify_add_watch {}: {}",
            path.display(),
            io::Error::last_os_error()
        );
    }

    pub(super) fn exit(&self, pid: u32) {
        let watch = ExitWatch::of(pid)
            .unwrap_or_else(|error| panic!("pidfd_open {pid}: {error}"))
            .unwrap_or_else(|| panic!("no process holds pid {pid}"));
        self.exits.borrow_mut().push(watch);
    }

    /// The next event, waiting for it. A file's change is reported ahead of an exit that is
    /// ready with it.
    pub(super) fn next(&self) -> Event {
        let mut exits = self.exits.borrow_mut();
        let mut polled: Vec<libc::pollfd> = std::iter::once(self.inotify.as_raw_fd())
            .chain(exits.iter().map(|watch| watch.as_raw_fd()))
            .map(|fd| libc::pollfd {
                fd,
                events: libc::POLLIN,
                revents: 0,
            })
            .collect();
        let count = libc::nfds_t::try_from(polled.len()).expect("a descriptor count");
        loop {
            // SAFETY: `polled` holds `count` live entries; no timeout.
            let ready = unsafe { libc::poll(polled.as_mut_ptr(), count, -1) };
            if ready > 0 {
                break;
            }
            let error = io::Error::last_os_error();
            assert_eq!(error.kind(), io::ErrorKind::Interrupted, "poll: {error}");
        }
        if polled[0].revents != 0 {
            self.drain();
            return Event::File;
        }
        let exited = polled[1..]
            .iter()
            .position(|entry| entry.revents != 0)
            .expect("poll answered for a descriptor it was given");
        drop(exits.remove(exited));
        Event::Exit
    }

    /// Read every queued inotify record: the changes behind one report are one event.
    fn drain(&self) {
        let mut buffer = [0_u8; 4096];
        loop {
            // SAFETY: `buffer` is writable for its full length; the descriptor is owned and
            // non-blocking, so the read returns at once.
            let read = unsafe {
                libc::read(
                    self.inotify.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                )
            };
            match read {
                0 => return,
                read if read > 0 => {}
                _ => {
                    let error = io::Error::last_os_error();
                    match error.kind() {
                        io::ErrorKind::WouldBlock => return,
                        io::ErrorKind::Interrupted => {}
                        _ => panic!("inotify read: {error}"),
                    }
                }
            }
        }
    }
}
