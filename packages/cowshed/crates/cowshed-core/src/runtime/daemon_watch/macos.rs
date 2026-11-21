//! The daemon's forks and exit on macOS: kqueue `EVFILT_PROC` with `NOTE_FORK` and `NOTE_EXIT`,
//! registered before the first listing, so the children are listed again after each later fork.

use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::time::Duration;

use super::Reported;
use crate::runtime::job_groups::Life;

/// The queue the daemon's `NOTE_FORK` and `NOTE_EXIT` are reported to, each once.
pub(super) struct Source {
    queue: OwnedFd,
}

impl Source {
    /// Every fork of the daemon is an event of this source.
    pub(super) const REPORTS_FORKS: bool = true;

    /// Watch `daemon`'s exit and forks from now; `None` once it has exited.
    pub(super) fn of(daemon: Life) -> io::Result<Option<Self>> {
        // SAFETY: kqueue takes nothing and returns a new descriptor.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh descriptor this function alone owns.
        let queue = unsafe { OwnedFd::from_raw_fd(queue) };
        let change = libc::kevent {
            ident: usize::try_from(daemon.pid).map_err(io::Error::other)?,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_CLEAR,
            fflags: libc::NOTE_FORK | libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        // SAFETY: one change read from live memory, no event list, on a live queue. With no room
        // for events a failed registration is the call's own error.
        let registered = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if registered != 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::ESRCH) => Ok(None),
                _ => Err(error),
            };
        }
        // Read after the registration, the pid still carries the daemon's unique id: the daemon
        // ran throughout, and the watch names it.
        if daemon.current()?.is_none() {
            return Ok(None);
        }
        Ok(Some(Self { queue }))
    }

    /// Wait up to `within` for the daemon's next event: what it reported, or `None` once
    /// `within` passed with none. A wait a signal interrupted reports nothing.
    pub(super) fn next(&mut self, within: Duration) -> io::Result<Option<Reported>> {
        let timeout = libc::timespec {
            tv_sec: libc::time_t::try_from(within.as_secs()).map_err(io::Error::other)?,
            tv_nsec: libc::c_long::from(within.subsec_nanos()),
        };
        // SAFETY: `event` is plain data the call may write; the queue and timeout are live.
        let (returned, event) = unsafe {
            let mut event = std::mem::zeroed::<libc::kevent>();
            let returned = libc::kevent(
                self.queue.as_raw_fd(),
                std::ptr::null(),
                0,
                &mut event,
                1,
                &timeout,
            );
            (returned, event)
        };
        match returned {
            ..0 => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
                Ok(Some(Reported::default()))
            }
            0 => Ok(None),
            _ if event.flags & libc::EV_ERROR != 0 => Err(i32::try_from(event.data)
                .map_or_else(io::Error::other, io::Error::from_raw_os_error)),
            _ => Ok(Some(Reported {
                forked: event.fflags & libc::NOTE_FORK != 0,
                exited: event.fflags & libc::NOTE_EXIT != 0,
            })),
        }
    }
}
