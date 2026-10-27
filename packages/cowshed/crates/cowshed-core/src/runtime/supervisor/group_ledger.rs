//! The supervisor's one group-ledger writer (specs/cowshed/11_shell.md §Supervisor recovery): a
//! dedicated thread that publishes the actor's immutable claim cuts in order and performs the
//! teardown handoff of the same ledger, so the actor never waits on the filesystem
//! (specs/cowshed/05_gateway.md §Deadlines).
//!
//! One cut may be in flight while one newer cut waits; a newer cut replaces the waiting one.
//! Nothing is queued, timed or retried: a failed write stays the writer's answer until a later cut
//! succeeds, and a failed final write is the final result. The recorder and the inherited
//! unresolved groups live only on this thread. The writer's clone of the socket's publication
//! lease is dropped after its last write and before its final answer, so none of its writes can
//! land once a successor holds the lease.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use crate::api::dto::JobId;
use crate::runtime::job_groups::{JobGroupClaim, Recorder, UnresolvedGroup};
use crate::runtime::supervisor_socket::PublicationLease;

use super::JobStateRecord;

/// One immutable cut of the actor's current claims. Each claim's member lives are the `Arc` the
/// job's observation published, shared as handed and never reloaded here.
type Cut = Vec<JobGroupClaim>;
type FinishReply = tokio::sync::oneshot::Sender<io::Result<()>>;

/// Runtime teardown's custody of every job, retired on the writer thread.
struct Custody {
    jobs: BTreeMap<JobId, JobStateRecord>,
    grace: Duration,
    done: std::sync::mpsc::SyncSender<io::Result<()>>,
}

/// The writer's admission and ordering, decided without I/O or a clock. `C` is a cut, `F` the
/// final flush's reply and `T` teardown custody, so the tables below drive it with plain values.
struct Slot<C, F, T> {
    /// The newest cut the writer has not taken; a newer one replaces it.
    latest: Option<C>,
    /// Whether the writer is writing a cut it took.
    in_flight: bool,
    /// Why the newest finished write failed, until a later write succeeds. Never retried.
    failure: Option<io::Error>,
    phase: Phase<F, T>,
}

enum Phase<F, T> {
    /// Cuts are admitted.
    Open,
    /// Admission is closed; the waiting cut is written, then the reply is answered.
    Finishing(F),
    /// Admission is closed and the custody waits for the in-flight write. A final flush reply
    /// still waiting is answered after the retirement.
    TearingDown { custody: T, finish: Option<F> },
    /// The writer took the custody; nothing else is written.
    Retiring,
    /// The final flush was answered: `ok` when the ledger holds the last cut.
    Closed { ok: bool },
    /// The writer thread ended without its final answer; nothing writes this ledger again.
    Dead,
}

/// What the writer does next: taken under the slot's lock, done outside it.
enum Work<C, F, T> {
    Write(C),
    /// Drop the lease, then answer the final flush with the newest write's outcome.
    Finish {
        reply: F,
        result: io::Result<()>,
    },
    Retire {
        custody: T,
        finish: Option<F>,
    },
}

/// How teardown custody was received.
enum Handoff<C, T> {
    /// Retired after the in-flight write; the obsolete waiting cut is returned to be dropped.
    Queued(Option<C>),
    /// A successful final flush already handed every claim off, and every job is terminal.
    Settled(T),
    /// No writer can take it: the caller keeps the claims.
    Refused(T),
}

impl<C, F, T> Slot<C, F, T> {
    fn new() -> Self {
        Self {
            latest: None,
            in_flight: false,
            failure: None,
            phase: Phase::Open,
        }
    }

    /// Replace the waiting cut, returning the one replaced. A closing writer refuses the cut.
    fn publish(&mut self, cut: C) -> Result<Option<C>, C> {
        match self.phase {
            Phase::Open => Ok(self.latest.replace(cut)),
            _ => Err(cut),
        }
    }

    /// Close admission: the waiting cut is written after the in-flight one, then `reply`.
    fn finish(&mut self, reply: F) -> Result<(), F> {
        match self.phase {
            Phase::Open => {
                self.phase = Phase::Finishing(reply);
                Ok(())
            }
            _ => Err(reply),
        }
    }

    /// Take the jobs as the full current authority: the waiting cut is obsolete.
    fn teardown(&mut self, custody: T, terminal: bool) -> Handoff<C, T> {
        match std::mem::replace(&mut self.phase, Phase::Retiring) {
            Phase::Open => {
                self.phase = Phase::TearingDown {
                    custody,
                    finish: None,
                };
                Handoff::Queued(self.latest.take())
            }
            Phase::Finishing(finish) => {
                self.phase = Phase::TearingDown {
                    custody,
                    finish: Some(finish),
                };
                Handoff::Queued(self.latest.take())
            }
            Phase::Closed { ok: true } if terminal => {
                self.phase = Phase::Closed { ok: true };
                Handoff::Settled(custody)
            }
            phase => {
                self.phase = phase;
                Handoff::Refused(custody)
            }
        }
    }

    /// The writer's next work once its in-flight write is done; `None` waits for an event.
    fn next(&mut self) -> Option<Work<C, F, T>> {
        if self.in_flight {
            return None;
        }
        if matches!(self.phase, Phase::Open | Phase::Finishing(_))
            && let Some(cut) = self.latest.take()
        {
            self.in_flight = true;
            return Some(Work::Write(cut));
        }
        match std::mem::replace(&mut self.phase, Phase::Retiring) {
            Phase::TearingDown { custody, finish } => Some(Work::Retire { custody, finish }),
            Phase::Finishing(reply) => {
                let result = self.failure.take().map_or(Ok(()), Err);
                self.phase = Phase::Closed { ok: result.is_ok() };
                Some(Work::Finish { reply, result })
            }
            phase => {
                self.phase = phase;
                None
            }
        }
    }

    /// The in-flight write ended. A failure is kept, not retried.
    fn written(&mut self, result: io::Result<()>) {
        self.in_flight = false;
        self.failure = result.err();
    }

    /// The writer thread ended. Returns the custody it never took; a closed writer stays closed.
    fn dead(&mut self) -> Option<T> {
        self.latest = None;
        match std::mem::replace(&mut self.phase, Phase::Dead) {
            Phase::TearingDown { custody, .. } => Some(custody),
            Phase::Closed { ok } => {
                self.phase = Phase::Closed { ok };
                None
            }
            Phase::Open | Phase::Finishing(_) | Phase::Retiring | Phase::Dead => None,
        }
    }

    fn closed_ok(&self) -> bool {
        matches!(self.phase, Phase::Closed { ok: true })
    }

    fn describe(&self) -> &'static str {
        match self.phase {
            Phase::Open => "is open",
            Phase::Finishing(_) => "is flushing its final cut",
            Phase::TearingDown { .. } => "holds runtime teardown custody",
            Phase::Retiring => "is retiring its jobs",
            Phase::Closed { ok: true } => "has closed",
            Phase::Closed { ok: false } => "closed after a failed final write",
            Phase::Dead => "ended without its final answer",
        }
    }
}

struct State {
    slot: Slot<Cut, FinishReply, Custody>,
    /// A written or replaced cut's cleared buffer, reused by the next cut.
    spare: Cut,
}

impl State {
    fn recycle(&mut self, cut: Cut) {
        debug_assert!(cut.is_empty(), "a recycled cut holds no member lives");
        if cut.capacity() > self.spare.capacity() {
            self.spare = cut;
        }
    }
}

struct Shared {
    /// Held only to move cuts, custody and answers; never through I/O.
    state: Mutex<State>,
    wake: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("group ledger slot poisoned")
    }
}

/// The actor's handle on its one ledger writer. The thread ends on its own after its final
/// answer; nothing on the actor joins it.
pub(super) struct GroupLedgerWriter {
    shared: Arc<Shared>,
    path: PathBuf,
}

impl GroupLedgerWriter {
    /// Start the writer before any job, so a spawn failure fails the supervisor's start.
    pub(super) fn start(
        recorder: Recorder,
        inherited: Vec<UnresolvedGroup>,
        lease: Option<PublicationLease>,
    ) -> io::Result<Self> {
        let path = recorder.path().to_path_buf();
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                slot: Slot::new(),
                spare: Vec::new(),
            }),
            wake: Condvar::new(),
        });
        let writer = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("cowshed-group-ledger".into())
            .spawn(move || write_ledger(&writer, recorder, inherited, lease))
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "cannot start the group ledger writer for {}: {error}",
                        path.display()
                    ),
                )
            })?;
        Ok(Self { shared, path })
    }

    /// Hand the current claims to the writer without waiting on its I/O: they replace any cut
    /// still waiting. Refused once the final flush or teardown has begun.
    pub(super) fn publish(&self, claims: impl Iterator<Item = JobGroupClaim>) -> io::Result<()> {
        let mut cut = std::mem::take(&mut self.shared.lock().spare);
        cut.extend(claims);
        let mut state = self.shared.lock();
        match state.slot.publish(cut) {
            Ok(replaced) => {
                if let Some(mut replaced) = replaced {
                    replaced.clear();
                    state.recycle(replaced);
                }
                drop(state);
                self.shared.wake.notify_one();
                Ok(())
            }
            Err(mut cut) => {
                cut.clear();
                state.recycle(cut);
                let error = self.refused(state.slot.describe());
                drop(state);
                Err(error)
            }
        }
    }

    /// Close admission and flush the waiting cut after the in-flight one. The answer comes
    /// after the writer dropped its lease, and no write follows it; a failed newest write is
    /// the answer's error.
    pub(super) fn finish(&mut self) -> tokio::sync::oneshot::Receiver<io::Result<()>> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        let mut state = self.shared.lock();
        match state.slot.finish(reply) {
            Ok(()) => {
                drop(state);
                self.shared.wake.notify_one();
            }
            Err(reply) => {
                let error = self.refused(state.slot.describe());
                drop(state);
                let _ = reply.send(Err(error));
            }
        }
        answer
    }

    /// Hand every job to this writer's thread: after its in-flight write it publishes the jobs,
    /// retires them natively, writes the handoff, drops the lease, then answers. A writer that
    /// cannot take them returns them, except that terminal jobs after a successful final flush
    /// are already handed off and drop here.
    pub(super) fn teardown(
        self,
        jobs: BTreeMap<JobId, JobStateRecord>,
        grace: Duration,
    ) -> Result<std::sync::mpsc::Receiver<io::Result<()>>, BTreeMap<JobId, JobStateRecord>> {
        let terminal = jobs.values().all(JobStateRecord::terminal);
        let (done, retired) = std::sync::mpsc::sync_channel(1);
        let handoff = self
            .shared
            .lock()
            .slot
            .teardown(Custody { jobs, grace, done }, terminal);
        match handoff {
            Handoff::Queued(obsolete) => {
                self.shared.wake.notify_one();
                drop(obsolete);
                Ok(retired)
            }
            Handoff::Settled(Custody { jobs, done, .. }) => {
                drop(jobs);
                let _ = done.send(Ok(()));
                Ok(retired)
            }
            Handoff::Refused(custody) => Err(custody.jobs),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    /// Whether the final flush answered Ok: the ledger holds the last cut and the lease is gone.
    pub(super) fn closed_ok(&self) -> bool {
        self.shared.lock().slot.closed_ok()
    }

    fn refused(&self, phase: &str) -> io::Error {
        io::Error::other(format!(
            "the group ledger writer for {} {phase}; it admits no further cut",
            self.path.display()
        ))
    }
}

/// A writer dropped without its final flush or teardown still ends: it writes the waiting cut
/// under its lease, drops the lease and exits, so no thread holds the lease for nobody.
impl Drop for GroupLedgerWriter {
    fn drop(&mut self) {
        let (reply, _) = tokio::sync::oneshot::channel();
        if self.shared.lock().slot.finish(reply).is_ok() {
            self.shared.wake.notify_one();
        }
    }
}

/// Marks the writer gone however its thread ends. Custody it never took stays held: releasing
/// those parents would turn a writer failure into a release of unproven membership.
struct Ended<'a>(&'a Shared);

impl Drop for Ended<'_> {
    fn drop(&mut self) {
        let untaken = self.0.lock().slot.dead();
        if let Some(custody) = untaken {
            cowshed_os::eprintln!(
                "cowshed: the group ledger writer ended before retiring {} jobs; their native claims and ledger entries remain held",
                custody.jobs.len()
            );
            std::mem::forget(custody.jobs);
        }
    }
}

fn write_ledger(
    shared: &Shared,
    recorder: Recorder,
    mut inherited: Vec<UnresolvedGroup>,
    lease: Option<PublicationLease>,
) {
    let _ended = Ended(shared);
    let mut state = shared.lock();
    loop {
        let Some(work) = state.slot.next() else {
            state = shared.wake.wait(state).expect("group ledger slot poisoned");
            continue;
        };
        drop(state);
        match work {
            Work::Write(mut cut) => {
                let result = record(&recorder, cut.iter().cloned(), &mut inherited);
                cut.clear();
                state = shared.lock();
                state.slot.written(result);
                state.recycle(cut);
            }
            Work::Finish { reply, result } => {
                drop(lease);
                // An actor gone before its answer already had every failure logged.
                let _ = reply.send(result);
                return;
            }
            Work::Retire {
                custody: Custody { jobs, grace, done },
                finish,
            } => {
                // The jobs are published before their retirement; a failure is logged and the
                // handoff write after retirement decides the result.
                let _ = record(&recorder, super::job_group_claims(&jobs), &mut inherited);
                let result = super::retire_dropped_jobs(jobs, Some(&recorder), inherited, grace);
                drop(lease);
                if let Some(reply) = finish {
                    let _ = reply.send(Err(io::Error::other(
                        "runtime teardown took over the final group ledger flush",
                    )));
                }
                let _ = done.send(result);
                return;
            }
        }
    }
}

/// One ledger rewrite. A success carries the inherited groups still held into the next one.
fn record(
    recorder: &Recorder,
    claims: impl Iterator<Item = JobGroupClaim> + Clone,
    inherited: &mut Vec<UnresolvedGroup>,
) -> io::Result<()> {
    match recorder.record(claims, inherited) {
        Ok(recorded) => {
            for error in &recorded.uninspected {
                cowshed_os::eprintln!(
                    "cowshed: an inherited unresolved process group in {} could not be \
                 inspected and is carried as it was: {error}",
                    recorder.path().display()
                );
            }
            *inherited = recorded.carried;
            Ok(())
        }
        Err(error) => {
            // The jobs run either way; what is lost is only the means to end them should this
            // supervisor die, which is worth saying where the daemon log shows it.
            cowshed_os::eprintln!(
                "cowshed: cannot record the job process groups at {}: {error}",
                recorder.path().display()
            );
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Table = Slot<&'static str, &'static str, &'static str>;

    #[derive(Clone, Copy)]
    enum Step {
        Publish(&'static str),
        Finish(&'static str),
        /// Custody, and whether every job in it is terminal.
        Teardown(&'static str, bool),
        Next,
        /// The in-flight write ended, with its failure if any.
        Written(Option<&'static str>),
        Dead,
        ClosedOk,
    }

    #[derive(Debug, PartialEq)]
    enum Seen {
        Published(Option<&'static str>),
        PublishRefused(&'static str),
        Finishing,
        FinishRefused(&'static str),
        Queued(Option<&'static str>),
        Settled(&'static str),
        Refused(&'static str),
        Idle,
        Write(&'static str),
        Finish(&'static str, Result<(), String>),
        Retire(&'static str, Option<&'static str>),
        Written,
        Dead(Option<&'static str>),
        ClosedOk(bool),
    }

    fn apply(slot: &mut Table, step: Step) -> Seen {
        match step {
            Step::Publish(cut) => match slot.publish(cut) {
                Ok(replaced) => Seen::Published(replaced),
                Err(cut) => Seen::PublishRefused(cut),
            },
            Step::Finish(reply) => match slot.finish(reply) {
                Ok(()) => Seen::Finishing,
                Err(reply) => Seen::FinishRefused(reply),
            },
            Step::Teardown(custody, terminal) => match slot.teardown(custody, terminal) {
                Handoff::Queued(obsolete) => Seen::Queued(obsolete),
                Handoff::Settled(custody) => Seen::Settled(custody),
                Handoff::Refused(custody) => Seen::Refused(custody),
            },
            Step::Next => match slot.next() {
                None => Seen::Idle,
                Some(Work::Write(cut)) => Seen::Write(cut),
                Some(Work::Finish { reply, result }) => {
                    Seen::Finish(reply, result.map_err(|error| error.to_string()))
                }
                Some(Work::Retire { custody, finish }) => Seen::Retire(custody, finish),
            },
            Step::Written(failure) => {
                slot.written(failure.map_or(Ok(()), |failure| Err(io::Error::other(failure))));
                Seen::Written
            }
            Step::Dead => Seen::Dead(slot.dead()),
            Step::ClosedOk => Seen::ClosedOk(slot.closed_ok()),
        }
    }

    fn run(table: &[(Step, Seen)]) {
        let mut slot = Table::new();
        for (index, (step, expected)) in table.iter().enumerate() {
            assert_eq!(&apply(&mut slot, *step), expected, "step {index}");
        }
    }

    #[test]
    fn cuts_published_during_a_write_coalesce_to_one_latest_successor() {
        run(&[
            (Step::Publish("a"), Seen::Published(None)),
            (Step::Next, Seen::Write("a")),
            (Step::Publish("b"), Seen::Published(None)),
            (Step::Publish("c"), Seen::Published(Some("b"))),
            (Step::Publish("d"), Seen::Published(Some("c"))),
            (Step::Next, Seen::Idle),
            (Step::Written(None), Seen::Written),
            (Step::Next, Seen::Write("d")),
            (Step::Written(None), Seen::Written),
            (Step::Next, Seen::Idle),
        ]);
    }

    #[test]
    fn a_failed_write_is_never_retried_and_is_the_final_answer() {
        run(&[
            (Step::Publish("a"), Seen::Published(None)),
            (Step::Next, Seen::Write("a")),
            (Step::Written(Some("disk full")), Seen::Written),
            (Step::Next, Seen::Idle),
            (Step::Finish("final"), Seen::Finishing),
            (
                Step::Next,
                Seen::Finish("final", Err("disk full".to_string())),
            ),
            (Step::ClosedOk, Seen::ClosedOk(false)),
            (Step::Teardown("jobs", true), Seen::Refused("jobs")),
            (Step::Next, Seen::Idle),
        ]);
    }

    #[test]
    fn a_later_successful_write_supersedes_an_earlier_failure() {
        run(&[
            (Step::Publish("a"), Seen::Published(None)),
            (Step::Next, Seen::Write("a")),
            (Step::Written(Some("disk full")), Seen::Written),
            (Step::Publish("b"), Seen::Published(None)),
            (Step::Next, Seen::Write("b")),
            (Step::Written(None), Seen::Written),
            (Step::Finish("final"), Seen::Finishing),
            (Step::Next, Seen::Finish("final", Ok(()))),
            (Step::ClosedOk, Seen::ClosedOk(true)),
            (
                Step::Teardown("terminal jobs", true),
                Seen::Settled("terminal jobs"),
            ),
        ]);
    }

    #[test]
    fn the_final_flush_writes_the_latest_cut_after_the_in_flight_one_and_admits_nothing_more() {
        run(&[
            (Step::Publish("a"), Seen::Published(None)),
            (Step::Next, Seen::Write("a")),
            (Step::Publish("b"), Seen::Published(None)),
            (Step::Finish("final"), Seen::Finishing),
            (Step::Publish("c"), Seen::PublishRefused("c")),
            (Step::Finish("again"), Seen::FinishRefused("again")),
            (Step::Next, Seen::Idle),
            (Step::Written(None), Seen::Written),
            (Step::Next, Seen::Write("b")),
            (Step::ClosedOk, Seen::ClosedOk(false)),
            (Step::Written(None), Seen::Written),
            (Step::Next, Seen::Finish("final", Ok(()))),
            (Step::ClosedOk, Seen::ClosedOk(true)),
            (Step::Next, Seen::Idle),
            (Step::Publish("d"), Seen::PublishRefused("d")),
            (Step::Dead, Seen::Dead(None)),
            (Step::ClosedOk, Seen::ClosedOk(true)),
        ]);
    }

    #[test]
    fn teardown_discards_the_latest_cut_and_retires_after_the_in_flight_write() {
        run(&[
            (Step::Publish("a"), Seen::Published(None)),
            (Step::Next, Seen::Write("a")),
            (Step::Publish("b"), Seen::Published(None)),
            (Step::Teardown("jobs", false), Seen::Queued(Some("b"))),
            (Step::Publish("c"), Seen::PublishRefused("c")),
            (Step::Finish("final"), Seen::FinishRefused("final")),
            (Step::Next, Seen::Idle),
            (Step::Written(Some("disk full")), Seen::Written),
            (Step::Next, Seen::Retire("jobs", None)),
            (Step::Next, Seen::Idle),
            (Step::Publish("d"), Seen::PublishRefused("d")),
        ]);
    }

    #[test]
    fn teardown_during_the_final_flush_takes_it_over_on_the_same_writer() {
        run(&[
            (Step::Publish("a"), Seen::Published(None)),
            (Step::Next, Seen::Write("a")),
            (Step::Publish("b"), Seen::Published(None)),
            (Step::Finish("final"), Seen::Finishing),
            (Step::Teardown("jobs", false), Seen::Queued(Some("b"))),
            (Step::Next, Seen::Idle),
            (Step::Written(None), Seen::Written),
            (Step::Next, Seen::Retire("jobs", Some("final"))),
        ]);
    }

    #[test]
    fn teardown_of_an_open_writer_retires_even_terminal_jobs_through_it() {
        run(&[
            (Step::Teardown("terminal jobs", true), Seen::Queued(None)),
            (Step::Next, Seen::Retire("terminal jobs", None)),
        ]);
    }

    #[test]
    fn a_closed_writer_returns_jobs_that_are_not_all_terminal() {
        run(&[
            (Step::Finish("final"), Seen::Finishing),
            (Step::Next, Seen::Finish("final", Ok(()))),
            (
                Step::Teardown("running jobs", false),
                Seen::Refused("running jobs"),
            ),
            (Step::ClosedOk, Seen::ClosedOk(true)),
        ]);
    }

    #[test]
    fn a_dead_writer_keeps_untaken_custody_and_refuses_everything() {
        run(&[
            (Step::Publish("a"), Seen::Published(None)),
            (Step::Next, Seen::Write("a")),
            (Step::Teardown("jobs", false), Seen::Queued(None)),
            (Step::Dead, Seen::Dead(Some("jobs"))),
            (Step::Publish("b"), Seen::PublishRefused("b")),
            (Step::Finish("final"), Seen::FinishRefused("final")),
            (
                Step::Teardown("more jobs", true),
                Seen::Refused("more jobs"),
            ),
            (Step::ClosedOk, Seen::ClosedOk(false)),
            (Step::Next, Seen::Idle),
        ]);
    }
}
