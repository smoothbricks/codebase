//! Doctor's progress on stderr (specs/cowshed/06_cli.md, "doctor"). Every probe is a
//! `doctor <probe>` lifecycle step: its `start` line always prints and its `done` line prints
//! once it took [`SLOW_PROBE`], so a hang is a `start` with no `done` and the elapsed times
//! printed are where a slow doctor's time went. A probe that waits on another party names it
//! before it asks ([`waits_on`]), and the per-workspace checks end in one summary line
//! ([`WorkspaceChecks`]). A caller that asked for its steps hears every step start and end.

use std::borrow::Cow;
use std::fmt;
use std::time::{Duration, Instant};

use cowshed_types::metadata::WorkspaceName;

/// How long a probe takes before its `done` line prints. On a loaded host every ordinary probe
/// answered in under a second, while the ones a slow doctor's time went to — the gateway's
/// answer, the store's metadata walk — took 2 s and more; below this a
/// `done` line would say only that a probe was fast, hundreds of times per project.
pub const SLOW_PROBE: Duration = Duration::from_secs(2);

/// Run one probe as a `doctor <name>` step.
pub async fn probe<T, E: fmt::Display>(
    name: impl Into<Cow<'static, str>>,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    cowshed_os::timing::timed_async_over("doctor", name, SLOW_PROBE, work).await
}

/// Run one probe that answers findings rather than a result as a `doctor <name>` step.
pub async fn probe_value<T>(
    name: impl Into<Cow<'static, str>>,
    work: impl Future<Output = T>,
) -> T {
    cowshed_os::timing::timed_value_over("doctor", name, SLOW_PROBE, work).await
}

/// Run one synchronous probe as a `doctor <name>` step.
pub fn probe_sync<T, E: fmt::Display>(
    name: fmt::Arguments<'_>,
    work: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    cowshed_os::timing::timed_over("doctor", name, SLOW_PROBE, work)
}

/// Name the party `probe` waits for, before it asks:
/// `cowshed: doctor <probe> waits-on=<party>`.
pub fn waits_on(probe: fmt::Arguments<'_>, party: impl fmt::Display) {
    cowshed_os::timing::attribute("doctor", probe, "waits-on", party);
}

/// The per-workspace checks, told as one line once they are all done:
/// `cowshed: doctor workspace-checks done elapsed=<d> workspaces=<n> slowest=<workspace> <d>`.
pub struct WorkspaceChecks {
    started: Instant,
    checked: usize,
    current: Option<(WorkspaceName, Instant)>,
    slowest: Option<(WorkspaceName, Duration)>,
}

impl WorkspaceChecks {
    pub fn start() -> Self {
        Self {
            started: Instant::now(),
            checked: 0,
            current: None,
            slowest: None,
        }
    }

    /// The checks of `workspace` begin, and those of the workspace before it are over.
    pub fn begin(&mut self, workspace: &WorkspaceName) {
        self.close();
        self.checked += 1;
        self.current = Some((workspace.clone(), Instant::now()));
    }

    fn close(&mut self) {
        if let Some((workspace, began)) = self.current.take() {
            let elapsed = began.elapsed();
            if self
                .slowest
                .as_ref()
                .is_none_or(|(_, slowest)| elapsed > *slowest)
            {
                self.slowest = Some((workspace, elapsed));
            }
        }
    }

    pub fn finish(mut self) {
        self.close();
        let elapsed = self.started.elapsed();
        match &self.slowest {
            Some((workspace, slowest)) => cowshed_os::eprintln!(
                "cowshed: doctor workspace-checks done elapsed={elapsed:?} workspaces={} slowest={workspace} {slowest:?}",
                self.checked
            ),
            None => {
                cowshed_os::eprintln!(
                    "cowshed: doctor workspace-checks done elapsed={elapsed:?} workspaces=0"
                )
            }
        }
    }
}
