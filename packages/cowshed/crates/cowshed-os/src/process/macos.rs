//! Darwin's kqueue `NOTE_EXIT` watch of a process's exit. A process's own facts are procinfo's.

use std::io;
use std::time::Duration;

use super::ExitWatch;

impl ExitWatch {
    /// [`Self::of`], answering `ESRCH` for a pid whose process has begun to exit, or that no
    /// process holds: Darwin refuses to watch an unreaped zombie (measured).
    pub fn register(pid: libc::pid_t) -> io::Result<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};

        // SAFETY: kqueue takes nothing and returns a new descriptor.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a fresh descriptor this function alone owns.
        let queue = unsafe { std::os::fd::OwnedFd::from_raw_fd(queue) };
        let change = libc::kevent {
            ident: usize::try_from(pid).map_err(io::Error::other)?,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
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
            return Err(io::Error::last_os_error());
        }
        Ok(Self(queue))
    }
}

impl ExitWatch {
    pub(super) fn exited(&self, within: Option<Duration>) -> io::Result<bool> {
        use std::os::fd::AsRawFd as _;

        let timeout = within
            .map(|within| {
                Ok::<_, io::Error>(libc::timespec {
                    tv_sec: libc::time_t::try_from(within.as_secs()).map_err(io::Error::other)?,
                    tv_nsec: libc::c_long::from(within.subsec_nanos()),
                })
            })
            .transpose()?;
        // SAFETY: `event` is plain data the call may write; the queue and timeout are live.
        let (returned, event) = unsafe {
            let mut event = std::mem::zeroed::<libc::kevent>();
            let returned = libc::kevent(
                self.0.as_raw_fd(),
                std::ptr::null(),
                0,
                &mut event,
                1,
                timeout
                    .as_ref()
                    .map_or(std::ptr::null(), std::ptr::from_ref),
            );
            (returned, event)
        };
        match returned {
            ..0 => Err(io::Error::last_os_error()),
            0 => Ok(false),
            _ if event.flags & libc::EV_ERROR != 0 => Err(i32::try_from(event.data)
                .map_or_else(io::Error::other, io::Error::from_raw_os_error)),
            _ => Ok(true),
        }
    }
}
