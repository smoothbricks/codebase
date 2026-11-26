//! What the supervisor's actor publishes for the heartbeats of its calls, `gateway status`, and
//! watch subscribers (specs/cowshed/05_gateway.md, "Deadlines" and "A land's gate takes tokens
//! back"): the authority it serves, how many turns it has completed, the turn it is in and since
//! when, whether a land's gate span is active, and every job it has not concluded (including each
//! job's withdrawn token count and `PRIO_DARWIN_BG` state).
//!
//! The actor is the one writer: it replaces an immutable snapshot on a [`tokio::sync::watch`]
//! channel as each turn starts and ends, as it admits a job (before the job's id is answered),
//! and as it forgets one. A heartbeat loads the latest without a round trip through the actor,
//! and so does a `hold`'s acknowledgement (11_shell.md, "Protocol"); a caller waiting for a state
//! change awaits `changed()` on its receiver instead of polling.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time::Instant;

use super::WorkspaceAuthoritySnapshot;
use crate::api::dto::{ExecClass, JobId};
use crate::runtime::supervisor_socket::JobPulse;

const fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Per-job state under a land's gate span, published alongside the supervisor's actor state for
/// heartbeats, `gateway status`, and watch subscribers (05_gateway.md "A land's gate takes tokens
/// back").
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobGatePriority {
    /// Token bytes currently withdrawn from this job's jobserver fifo while a land's gate runs.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub tokens_withdrawn: u32,
    /// Whether `PRIO_DARWIN_BG` is currently applied to this job's process tree.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub darwinbg_active: bool,
    /// Completed `PRIO_DARWIN_BG` clears for this job (monotonically incremented on each clear,
    /// so a watcher distinguishes "never set" from "set and cleared").
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub darwinbg_cleared: u64,
    /// Completed macOS `PRIO_DARWIN_BG` evaluation windows for this job (monotonically incremented
    /// each time a 3 s window decides a step).
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub darwinbg_windows: u64,
}

impl JobGatePriority {
    /// Whether no land-gate withdrawal or `PRIO_DARWIN_BG` transition has occurred on this job.
    pub(crate) const fn is_default(&self) -> bool {
        self.tokens_withdrawn == 0
            && !self.darwinbg_active
            && self.darwinbg_cleared == 0
            && self.darwinbg_windows == 0
    }
}

/// The turn the actor is in: what woke it, and when it started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Turn {
    pub(crate) kind: &'static str,
    pub(crate) since: Instant,
}

/// A job the actor has not concluded, as it publishes it: what a heartbeat about it reports, and
/// the class `gateway status` counts it by.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LiveJob {
    pub(crate) pulse: JobPulse,
    pub(crate) class: ExecClass,
}

/// One published state of the actor.
#[derive(Debug)]
pub struct Liveness {
    /// The authority it serves now; an advance publishes the new one before it answers.
    pub(crate) authority: Arc<WorkspaceAuthoritySnapshot>,
    /// The turns it has completed.
    pub(crate) turns: u64,
    /// The turn it is in, or `None` while it waits for its next command or event.
    pub(crate) turn: Option<Turn>,
    /// Whether the supervisor currently sees an active land's gate span on `gate-watch`.
    gate_active: bool,
    /// Every job it has not concluded, by job id, as of its last completed turn, and each job the
    /// turn it is in has admitted, less each it has forgotten.
    jobs: Arc<[LiveJob]>,
}

impl Liveness {
    /// Whether the supervisor currently sees an active land's gate span on `gate-watch`.
    pub fn gate_active(&self) -> bool {
        self.gate_active
    }

    /// Per-job land-gate withdrawal and `PRIO_DARWIN_BG` state for `job_id`, if unconcluded.
    pub fn job_gate_priority(&self, job_id: JobId) -> Option<JobGatePriority> {
        self.jobs
            .binary_search_by_key(&job_id, |job| job.pulse.job_id)
            .ok()
            .map(|at| self.jobs[at].pulse.gate_priority)
    }

    /// `job_id` as of the actor's last completed turn or its admission, if it has not concluded.
    pub(crate) fn job(&self, job_id: JobId) -> Option<JobPulse> {
        self.jobs
            .binary_search_by_key(&job_id, |job| job.pulse.job_id)
            .ok()
            .map(|at| self.jobs[at].pulse)
    }

    /// Every job it has not concluded, in job id order.
    pub(crate) fn jobs(&self) -> &[LiveJob] {
        &self.jobs
    }
}

/// Where one actor publishes its liveness: cloned into every handle of that actor.
#[derive(Clone, Debug)]
pub(crate) struct LivenessSlot(watch::Receiver<Arc<Liveness>>);

impl LivenessSlot {
    pub(crate) fn watch(&self) -> watch::Receiver<Arc<Liveness>> {
        self.0.clone()
    }
}

/// The actor's side of its [`LivenessSlot`]. Each publication allocates the snapshot alone:
/// the authority and the job list are shared with the last one until they change.
pub(super) struct LivenessWriter {
    tx: watch::Sender<Arc<Liveness>>,
    authority: Arc<WorkspaceAuthoritySnapshot>,
    turns: u64,
    turn: Option<Turn>,
    gate_active: bool,
    jobs: Arc<[LiveJob]>,
}

impl LivenessWriter {
    /// An actor that serves `authority`, has run no turn and holds no job.
    pub(super) fn new(authority: &WorkspaceAuthoritySnapshot) -> (Self, LivenessSlot) {
        let authority = Arc::new(authority.clone());
        let jobs: Arc<[LiveJob]> = Arc::from(Vec::new());
        let initial = Arc::new(Liveness {
            authority: Arc::clone(&authority),
            turns: 0,
            turn: None,
            gate_active: false,
            jobs: Arc::clone(&jobs),
        });
        let (tx, rx) = watch::channel(initial);
        let writer = Self {
            tx,
            authority,
            turns: 0,
            turn: None,
            gate_active: false,
            jobs,
        };
        (writer, LivenessSlot(rx))
    }

    /// A turn handling `kind` starts now.
    pub(super) fn begin(&mut self, kind: &'static str) {
        self.turn = Some(Turn {
            kind,
            since: Instant::now(),
        });
        self.publish();
    }

    /// The turn holds `jobs` unconcluded now, in job id order: published before it answers an
    /// admitted job's id or forgets a job, so a call about one reads it as the actor holds it while
    /// the turn goes on.
    pub(super) fn publish_jobs(
        &mut self,
        gate_active: bool,
        jobs: impl Iterator<Item = LiveJob> + Clone,
    ) {
        let gate_changed = self.gate_active != gate_active;
        self.gate_active = gate_active;
        if self.hold(jobs) || gate_changed {
            self.publish();
        }
    }

    /// The turn ended with `jobs` unconcluded, in job id order.
    pub(super) fn end(&mut self, gate_active: bool, jobs: impl Iterator<Item = LiveJob> + Clone) {
        self.turn = None;
        self.turns += 1;
        self.gate_active = gate_active;
        self.hold(jobs);
        self.publish();
    }

    /// Hold `jobs` from now on; whether they differ from those held. An unchanged list is kept,
    /// so publications share it until it changes.
    fn hold(&mut self, jobs: impl Iterator<Item = LiveJob> + Clone) -> bool {
        let changed = !self.jobs.iter().copied().eq(jobs.clone());
        if changed {
            self.jobs = jobs.collect();
        }
        changed
    }

    /// The actor serves `authority` from now on.
    pub(super) fn serve(&mut self, authority: &WorkspaceAuthoritySnapshot) {
        if *self.authority != *authority {
            self.authority = Arc::new(authority.clone());
            self.publish();
        }
    }

    fn publish(&self) {
        let _ = self.tx.send_replace(Arc::new(Liveness {
            authority: Arc::clone(&self.authority),
            turns: self.turns,
            turn: self.turn,
            gate_active: self.gate_active,
            jobs: Arc::clone(&self.jobs),
        }));
    }
}
