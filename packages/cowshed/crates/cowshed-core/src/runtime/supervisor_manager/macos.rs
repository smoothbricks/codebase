//! How the manager waits for a supervisor that is not its child on macOS: a one-shot kqueue
//! `NOTE_EXIT` registration.

use std::io;

/// Block until `pid`, which is not this process's child, exits. A process already gone, or
/// already exiting, has exited.
pub(super) fn wait_for_exit(pid: u32) -> io::Result<()> {
    let ident = usize::try_from(pid).map_err(io::Error::other)?;
    // SAFETY: kqueue returns a new descriptor this function owns and closes.
    let queue = unsafe { libc::kqueue() };
    if queue < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kevent structs are plain data that outlive the call; `queue` is live until
    // the close below.
    let result = unsafe {
        let change = libc::kevent {
            ident,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let mut event = std::mem::zeroed::<libc::kevent>();
        let returned = libc::kevent(queue, &change, 1, &mut event, 1, std::ptr::null());
        let outcome = match returned {
            ..0 => Err(io::Error::last_os_error()),
            // A registration that failed comes back as an event carrying the error.
            _ if event.flags & libc::EV_ERROR != 0 => Err(i32::try_from(event.data)
                .map_or_else(io::Error::other, io::Error::from_raw_os_error)),
            _ => Ok(()),
        };
        libc::close(queue);
        outcome
    };
    // ESRCH: it already exited, or began to, and its descriptors may still be open; a
    // supervisor this manager signals is therefore watched before it is (`ExitWatch`).
    match result {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        other => other,
    }
}
