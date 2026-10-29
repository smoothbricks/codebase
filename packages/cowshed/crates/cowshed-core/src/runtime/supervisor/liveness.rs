//! What the supervisor's actor publishes for the heartbeats of its calls (specs/cowshed/
//! 05_gateway.md, "Deadlines"): the authority it serves, how many turns it has completed, the
//! turn it is in and since when, and every job it has not concluded as a heartbeat reports it.
//! The actor is the one writer: it swaps an immutable snapshot as each turn starts and ends, and
//! a heartbeat loads the latest without a lock and without a round trip through the actor. A
//! turn that waits on the blocking pool therefore keeps every heartbeat going and names itself
//! in each of them; a turn that never ends is told apart from an idle actor by its rising age
//! and a turn count that stays put.

use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::time::Instant;

use super::WorkspaceAuthoritySnapshot;
use crate::api::dto::JobId;
use crate::runtime::supervisor_socket::JobPulse;

/// The turn the actor is in: what woke it, and when it started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Turn {
    pub(crate) kind: &'static str,
    pub(crate) since: Instant,
}

/// One published state of the actor.
#[derive(Debug)]
pub(crate) struct Liveness {
    /// The authority it serves now; an advance publishes the new one before it answers.
    pub(crate) authority: Arc<WorkspaceAuthoritySnapshot>,
    /// The turns it has completed.
    pub(crate) turns: u64,
    /// The turn it is in, or `None` while it waits for its next command or event.
    pub(crate) turn: Option<Turn>,
    /// Every job it has not concluded, by job id, as of its last completed turn.
    jobs: Arc<[JobPulse]>,
}

impl Liveness {
    /// `job_id` as of the actor's last completed turn, if it has not concluded.
    pub(crate) fn job(&self, job_id: JobId) -> Option<JobPulse> {
        self.jobs
            .binary_search_by_key(&job_id, |pulse| pulse.job_id)
            .ok()
            .map(|at| self.jobs[at])
    }
}

/// Where one actor publishes its liveness: cloned into every handle of that actor.
#[derive(Clone, Debug)]
pub(crate) struct LivenessSlot(Arc<ArcSwap<Liveness>>);

impl LivenessSlot {
    pub(crate) fn load(&self) -> Arc<Liveness> {
        self.0.load_full()
    }
}

/// The actor's side of its [`LivenessSlot`]. Each publication allocates the snapshot alone:
/// the authority and the job list are shared with the last one until they change.
pub(super) struct LivenessWriter {
    slot: LivenessSlot,
    authority: Arc<WorkspaceAuthoritySnapshot>,
    turns: u64,
    turn: Option<Turn>,
    jobs: Arc<[JobPulse]>,
}

impl LivenessWriter {
    /// An actor that serves `authority`, has run no turn and holds no job.
    pub(super) fn new(authority: &WorkspaceAuthoritySnapshot) -> (Self, LivenessSlot) {
        let authority = Arc::new(authority.clone());
        let jobs: Arc<[JobPulse]> = Arc::from(Vec::new());
        let slot = LivenessSlot(Arc::new(ArcSwap::from_pointee(Liveness {
            authority: Arc::clone(&authority),
            turns: 0,
            turn: None,
            jobs: Arc::clone(&jobs),
        })));
        let writer = Self {
            slot: slot.clone(),
            authority,
            turns: 0,
            turn: None,
            jobs,
        };
        (writer, slot)
    }

    /// A turn handling `kind` starts now.
    pub(super) fn begin(&mut self, kind: &'static str) {
        self.turn = Some(Turn {
            kind,
            since: Instant::now(),
        });
        self.publish();
    }

    /// The turn ended with `jobs` unconcluded, in job id order. The job list is replaced only
    /// when it changed.
    pub(super) fn end(&mut self, jobs: impl Iterator<Item = JobPulse> + Clone) {
        self.turn = None;
        self.turns += 1;
        if !self.jobs.iter().copied().eq(jobs.clone()) {
            self.jobs = jobs.collect();
        }
        self.publish();
    }

    /// The actor serves `authority` from now on.
    pub(super) fn serve(&mut self, authority: &WorkspaceAuthoritySnapshot) {
        if *self.authority != *authority {
            self.authority = Arc::new(authority.clone());
            self.publish();
        }
    }

    fn publish(&self) {
        self.slot.0.store(Arc::new(Liveness {
            authority: Arc::clone(&self.authority),
            turns: self.turns,
            turn: self.turn,
            jobs: Arc::clone(&self.jobs),
        }));
    }
}
