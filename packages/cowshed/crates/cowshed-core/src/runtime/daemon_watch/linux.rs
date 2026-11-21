//! The daemon's exit on Linux: the pidfd proven to name its life, readable once it has exited.
//! Linux reports no fork to an unprivileged watcher, so the daemon's children are listed when
//! the watch is made and each time its caller follows the forks, never on an event: a child the
//! daemon forks after the last listing is unseen. Where the daemon is a job's, it and every
//! worker it forks sit in the job's cgroup, whose retirement ends such a worker.

use std::io;
use std::time::Duration;

use cowshed_os::process::ExitWatch;

use super::Reported;
use crate::runtime::job_groups::Life;

/// The daemon's exit watch.
pub(super) struct Source {
    exit: ExitWatch,
}

impl Source {
    /// No fork of the daemon is an event of this source.
    pub(super) const REPORTS_FORKS: bool = false;

    /// Watch `daemon`'s exit from now; `None` once it has exited.
    pub(super) fn of(daemon: Life) -> io::Result<Option<Self>> {
        Ok(daemon.watch_exit()?.map(|exit| Self { exit }))
    }

    /// Wait up to `within` for the daemon's exit: that it exited, or `None` once `within` passed
    /// while it still ran.
    pub(super) fn next(&mut self, within: Duration) -> io::Result<Option<Reported>> {
        Ok(self.exit.within(within)?.then_some(Reported {
            forked: false,
            exited: true,
        }))
    }
}
