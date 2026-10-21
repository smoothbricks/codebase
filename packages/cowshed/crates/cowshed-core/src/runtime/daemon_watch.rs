//! An Nx daemon's exit and the children it forked, watched from before the daemon is stopped, so
//! its stop ends its plugin workers with it (04_sandbox.md: "its plugin workers go with it").
//!
//! Nx spawns each isolated plugin worker detached (pinned Nx 23.2.1,
//! `project-graph/plugins/isolation/isolated-plugin.js`), so a worker leads its own session and
//! group: neither the job's ledgered group nor the daemon's session names it. When the daemon
//! exits, launchd adopts its children, and from then on each one's kernel record names launchd as
//! its parent. Nothing the kernel keeps then ties a worker to the daemon, and a look for
//! launchd's children would find every other orphan of the host. So the set is captured while the
//! workers are still the daemon's children, from the kernel's own bookkeeping: the children of
//! the running daemon (`proc_listchildpids`), each kept only when its record names the daemon's
//! `p_uniqueid` as its parent's, and held by its own unique id ([`Life`]) with its exit watched
//! from then. A `NOTE_FORK` watch on the daemon, registered before the first listing, lists the
//! children again after each later fork. A listing that ends with the daemon gone may have
//! missed a child it forked as it exited; that is said, never guessed.
//!
//! Once the daemon has exited, each child still running is sent `SIGTERM` through its identity,
//! and one that outlives the caller's grace `SIGKILL`. Every wait is on the kernel's exit event;
//! the grace only bounds a hang.
//!
//! The keeper's daemon is a job of its supervisor, and a job's end -- its group stopped, or the
//! daemon gone on its own -- ends none of the workers, which left the group. The job's process
//! observation followed each of them from its birth, while it was the daemon's child
//! ([`super::process_monitor`]): what it saw born and not yet exit is ended the same way
//! ([`Orphans::of`]).

use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::time::{Duration, Instant};

use super::job_groups::{ExitWatch, Life};
use super::process_events::{Darwin, ProcessReads as _};

/// A daemon's exit and the children it forked, watched from a moment it ran.
pub(crate) struct DaemonWatch {
    daemon: Life,
    /// `NOTE_FORK` and `NOTE_EXIT` of the daemon, each reported once.
    queue: OwnedFd,
    children: Vec<Child>,
    exited: bool,
    /// A listing ended with the daemon gone, so a child it forked may be unseen.
    incomplete: bool,
}

/// One child of the daemon, held from while it was the daemon's.
struct Child {
    life: Life,
    exit: ExitWatch,
}

/// The children of a daemon that has exited, which may still run.
/// For the keeper's daemon job, every life of the job that may still run.
pub(crate) struct Orphans {
    daemon: i32,
    children: Vec<Child>,
    incomplete: bool,
}

impl DaemonWatch {
    /// Watch `daemon`'s exit and forks from now, holding the children it has; `None` once it has
    /// exited. Registered before any signal is sent.
    pub(crate) fn of(daemon: Life) -> io::Result<Option<Self>> {
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
        let mut watch = Self {
            daemon,
            queue,
            children: Vec::new(),
            exited: false,
            incomplete: false,
        };
        watch.list()?;
        Ok(Some(watch))
    }

    /// Hold each child the daemon has now that is not yet held. A pid is the daemon's child only
    /// when its record, one read, names the daemon's unique id as its parent's: a pid reused since
    /// the listing names a stranger, and one launchd adopted names launchd.
    fn list(&mut self) -> io::Result<()> {
        let parent = u32::try_from(self.daemon.pid).map_err(io::Error::other)?;
        for pid in Darwin.children(parent)? {
            let pid = i32::try_from(pid).map_err(io::Error::other)?;
            let Some(record) = super::job_groups::process_record(pid)? else {
                continue;
            };
            let life = record.life();
            if record.parent_unique_id != self.daemon.unique_id
                || self.children.iter().any(|child| child.life == life)
            {
                continue;
            }
            // One that exited since the listing leaves nothing to end.
            if let Some(exit) = life.watch_exit()? {
                self.children.push(Child { life, exit });
            }
        }
        // The kernel gives the daemon's children to launchd as it exits, so a listing that ends
        // with the daemon gone may have missed one.
        if self.daemon.current()?.is_none() {
            self.incomplete = true;
        }
        Ok(())
    }

    /// Take the forks, and the exit, the daemon has reported so far, without waiting.
    pub(crate) fn follow_forks(&mut self) -> io::Result<()> {
        self.take(Duration::ZERO).map(drop)
    }

    /// `SIGTERM` the daemon through its identity: `Ok(false)` once it no longer runs.
    pub(crate) fn terminate(&self) -> io::Result<bool> {
        match self.daemon.current()? {
            Some(daemon) => daemon.signal(libc::SIGTERM),
            None => Ok(false),
        }
    }

    /// Wait up to `within` for the daemon to exit, holding each child it forks meanwhile: its
    /// orphans once it has, or this watch while it still runs.
    pub(crate) fn await_exit(mut self, within: Duration) -> io::Result<Result<Orphans, Self>> {
        Ok(if self.take(within)? {
            Ok(Orphans {
                daemon: self.daemon.pid,
                children: self.children,
                incomplete: self.incomplete,
            })
        } else {
            Err(self)
        })
    }

    /// Whether the daemon exited within `within`, listing its children after each fork it
    /// reports meanwhile.
    fn take(&mut self, within: Duration) -> io::Result<bool> {
        let deadline = Instant::now() + within;
        while !self.exited {
            let left = deadline.saturating_duration_since(Instant::now());
            let timeout = libc::timespec {
                tv_sec: libc::time_t::try_from(left.as_secs()).map_err(io::Error::other)?,
                tv_nsec: libc::c_long::from(left.subsec_nanos()),
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
                }
                0 => return Ok(false),
                _ if event.flags & libc::EV_ERROR != 0 => {
                    return Err(i32::try_from(event.data)
                        .map_or_else(io::Error::other, io::Error::from_raw_os_error));
                }
                _ => {
                    if event.fflags & libc::NOTE_FORK != 0 {
                        self.list()?;
                    }
                    if event.fflags & libc::NOTE_EXIT != 0 {
                        self.exited = true;
                    }
                }
            }
        }
        Ok(true)
    }
}

impl Orphans {
    /// The lives of an exited daemon's job that its process observation saw born and not yet
    /// exit ([`super::process_monitor::ProcessMonitor::running`]), each watched from now by its
    /// unique id, the observation's birth token. One that has exited since leaves nothing to end.
    pub(crate) fn of(
        daemon: i32,
        lives: Vec<super::process_tree::ProcessIdentity>,
    ) -> io::Result<Self> {
        let mut children = Vec::new();
        for life in lives {
            let life = Life {
                pid: life.pid.cast_signed(),
                unique_id: life.birth.0,
            };
            if let Some(exit) = life.watch_exit()? {
                children.push(Child { life, exit });
            }
        }
        Ok(Self {
            daemon,
            children,
            incomplete: false,
        })
    }
    /// End every child of the exited daemon still running: `SIGTERM`, then `SIGKILL` to one that
    /// outlives `grace`, each waited for on its exit event. Answers, one line each, what it could
    /// not end or not tell; nothing once every child has exited.
    pub(crate) fn end(self, grace: Duration) -> Vec<String> {
        let daemon = self.daemon;
        let mut said = Vec::new();
        if self.incomplete {
            said.push(format!(
                "the Nx daemon {daemon} exited while its children were listed, so a plugin \
                 worker it forked then may be unseen and outlive it"
            ));
        }
        let mut running = self.children;
        for (signal, name) in [(libc::SIGTERM, "SIGTERM"), (libc::SIGKILL, "SIGKILL")] {
            for child in &running {
                let pid = child.life.pid;
                if let Err(error) = child
                    .life
                    .current()
                    .and_then(|process| process.map_or(Ok(false), |process| process.signal(signal)))
                {
                    said.push(format!(
                        "sending {name} to plugin worker {pid} of the Nx daemon {daemon} failed: \
                         {error}"
                    ));
                }
            }
            let deadline = Instant::now() + grace;
            running.retain(|child| {
                match child
                    .exit
                    .within(deadline.saturating_duration_since(Instant::now()))
                {
                    Ok(exited) => !exited,
                    Err(error) => {
                        said.push(format!(
                            "waiting for plugin worker {} of the Nx daemon {daemon} to exit \
                             failed: {error}",
                            child.life.pid
                        ));
                        false
                    }
                }
            });
        }
        for child in &running {
            said.push(format!(
                "plugin worker {} of the Nx daemon {daemon} outlived SIGKILL for {}s",
                child.life.pid,
                grace.as_secs()
            ));
        }
        said
    }
}
