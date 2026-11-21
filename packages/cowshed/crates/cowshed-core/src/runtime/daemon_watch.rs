//! An Nx daemon's exit and the children it forked, watched from before the daemon is stopped, so
//! its stop ends its plugin workers with it (04_sandbox.md: "its plugin workers go with it").
//!
//! Nx spawns each isolated plugin worker detached (pinned Nx 24.0.6,
//! `project-graph/plugins/isolation/isolated-plugin.js`), so a worker leads its own session and
//! group: neither the job's ledgered group nor the daemon's session names it. When the daemon
//! exits, the host's reaper adopts its children (macOS: launchd; Linux: init or a subreaper),
//! and from then on each one's kernel record names that reaper as its parent. Nothing the kernel
//! keeps then ties a worker to the daemon, and a look for the reaper's children would find every
//! other orphan of the host. So the set is captured while the workers are still the daemon's
//! children, from the kernel's own bookkeeping: the children of the running daemon, each kept
//! only when its record names the daemon's life as its parent's, and held by its own life
//! ([`Life`]) with its exit watched from then. The watch of the daemon is the platform's
//! (`macos.rs`: kqueue `NOTE_FORK` and `NOTE_EXIT`, registered before the first listing, so the
//! children are listed again after each later fork; `linux.rs`: a pidfd, which reports the exit
//! alone, so the children are listed when the watch is made and each time its caller follows
//! the forks). A listing that ends with the daemon gone may have missed a child it forked as
//! it exited; that is said, never guessed.
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
use std::time::{Duration, Instant};

use cowshed_os::process::ExitWatch;

use super::job_groups::Life;
use super::process_events::{Darwin, ProcessReads as _};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

/// What the daemon's watch reported at once.
#[derive(Clone, Copy, Debug, Default)]
struct Reported {
    /// It forked an uncounted number of children.
    forked: bool,
    exited: bool,
}

/// A daemon's exit and the children it forked, watched from a moment it ran.
pub(crate) struct DaemonWatch {
    daemon: Life,
    /// The daemon's exit, and its forks where the platform reports them.
    source: platform::Source,
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
        let Some(source) = platform::Source::of(daemon)? else {
            return Ok(None);
        };
        let mut watch = Self {
            daemon,
            source,
            children: Vec::new(),
            exited: false,
            incomplete: false,
        };
        watch.list()?;
        Ok(Some(watch))
    }

    /// Hold each child the daemon has now that is not yet held. A pid is the daemon's child only
    /// when its record, one read, names the daemon's unique id as its parent's: a pid reused since
    /// the listing names a stranger, and one the host's reaper adopted names that reaper.
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
        // The kernel gives the daemon's children to the host's reaper as it exits, so a listing
        // that ends with the daemon gone may have missed one.
        if self.daemon.current()?.is_none() {
            self.incomplete = true;
        }
        Ok(())
    }

    /// Take the forks, and the exit, the daemon has reported so far, without waiting. Under a
    /// watch that reports no fork, this is where a running daemon's children are listed again.
    pub(crate) fn follow_forks(&mut self) -> io::Result<()> {
        if !self.take(Duration::ZERO)? && !platform::Source::REPORTS_FORKS {
            self.list()?;
        }
        Ok(())
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
            let Some(reported) = self.source.next(left)? else {
                return Ok(false);
            };
            if reported.forked {
                self.list()?;
            }
            if reported.exited {
                self.exited = true;
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
