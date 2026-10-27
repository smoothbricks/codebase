//! Cancellable owned-vnode waits shared by host fixture binaries (08_testing.md, Fixtures).

use std::fs;
use std::path::Path;

/// Recheck owned state only when its vnode changes or its cancellation pipe closes. Registration
/// precedes the first look, so a write between the look and the wait cannot be missed.
pub fn await_vnode(
    path: &Path,
    cancel: Option<std::io::PipeReader>,
    mut ready: impl FnMut() -> bool,
) -> bool {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let directory = fs::File::open(path).expect("open the watched vnode");
    // SAFETY: kqueue takes no arguments; a negative answer is an error.
    let queue = unsafe { libc::kqueue() };
    assert!(queue >= 0, "kqueue: {}", std::io::Error::last_os_error());
    // SAFETY: `queue` is a new descriptor this function alone owns.
    let queue = unsafe { OwnedFd::from_raw_fd(queue) };
    let cancel_id = cancel
        .as_ref()
        .map(|cancel| usize::try_from(cancel.as_raw_fd()).expect("a descriptor is nonnegative"));
    let mut changes = vec![libc::kevent {
        ident: directory.as_raw_fd() as libc::uintptr_t,
        filter: libc::EVFILT_VNODE,
        flags: libc::EV_ADD | libc::EV_CLEAR,
        fflags: libc::NOTE_WRITE,
        data: 0,
        udata: std::ptr::null_mut(),
    }];
    if let Some(ident) = cancel_id {
        changes.push(libc::kevent {
            ident,
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD,
            fflags: 0,
            data: 0,
            udata: std::ptr::null_mut(),
        });
    }
    // SAFETY: valid vnode and optional pipe changes, no events requested back, no timeout.
    let registered = unsafe {
        libc::kevent(
            queue.as_raw_fd(),
            changes.as_ptr(),
            libc::c_int::try_from(changes.len()).expect("one or two event filters"),
            std::ptr::null_mut(),
            0,
            std::ptr::null(),
        )
    };
    assert_eq!(
        registered,
        0,
        "watch {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
    loop {
        if ready() {
            return true;
        }
        // SAFETY: an all-zero `kevent` is a valid value of the plain C struct.
        let mut event: libc::kevent = unsafe { std::mem::zeroed() };
        // SAFETY: no changes, room for one event out, no timeout: block until one fires.
        let fired = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                std::ptr::null(),
                0,
                &mut event,
                1,
                std::ptr::null(),
            )
        };
        if fired == 1 && cancel_id.is_some_and(|ident| event.ident == ident) {
            return ready();
        }
        if fired < 0 {
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.raw_os_error(),
                Some(libc::EINTR),
                "watch {}: {error}",
                path.display()
            );
        }
    }
}
