//! The supervisor's one group-ledger writer (specs/cowshed/11_shell.md §Supervisor recovery): a
//! dedicated thread that publishes the actor's immutable claim cuts in order and performs the
//! teardown handoff of the same ledger, so the actor never waits on the filesystem
//! (specs/cowshed/05_gateway.md §Deadlines).
//!
//! One cut may be in flight while one newer cut waits; a newer cut replaces the waiting one.
//! Ordinary publication is bounded to at most once per [`MIN_PUBLISH_INTERVAL`], and cuts whose
//! serialized bytes match the last published bytes skip temp+rename. Final retirement and runtime
//! teardown bypass the rate bound and flush at once. Nothing is queued or retried: a failed write
//! stays the writer's answer until a later cut succeeds, and a failed final write is the final
//! result. The recorder and the inherited unresolved groups live only on this thread. The writer's
//! clone of the socket's publication lease is dropped after its last write and before its final
//! answer, so none of its writes can land once a successor holds the lease.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::api::dto::JobId;
use crate::runtime::job_groups::{JobGroupClaim, Recorder, UnresolvedGroup};
use crate::runtime::process_tree::ProcessIdentity;
use crate::runtime::supervisor_socket::PublicationLease;

use super::JobStateRecord;

/// Minimum interval between ordinary ledger publications on disk (11_shell.md §Supervisor
/// recovery). Final retirement, runtime teardown, and `PRIO_DARWIN_BG` durability flushes bypass
/// this bound and flush at once.
const MIN_PUBLISH_INTERVAL: Duration = Duration::from_secs(1);

/// One immutable cut of the actor's current claims and active `PRIO_DARWIN_BG` lives. Each
/// claim's member lives are the `Arc` the job's observation published, shared as handed and never
/// reloaded here.
struct Cut {
    claims: Vec<JobGroupClaim>,
    darwinbg: Vec<ProcessIdentity>,
    /// Optional completion reply when a caller requires the cut on disk before acting (such as
    /// persisting lives before setting `PRIO_DARWIN_BG`).
    ack: Option<tokio::sync::oneshot::Sender<io::Result<()>>>,
}

impl Cut {
    fn new() -> Self {
        Self {
            claims: Vec::new(),
            darwinbg: Vec::new(),
            ack: None,
        }
    }

    fn clear(&mut self) {
        self.claims.clear();
        self.darwinbg.clear();
        self.ack = None;
    }

    fn is_empty(&self) -> bool {
        self.claims.is_empty() && self.darwinbg.is_empty() && self.ack.is_none()
    }
}

type FinishReply = tokio::sync::oneshot::Sender<io::Result<()>>;

/// Runtime teardown's custody of every job, retired on the writer thread.
struct Custody {
    jobs: BTreeMap<JobId, JobStateRecord>,
    grace: Duration,
    done: std::sync::mpsc::SyncSender<io::Result<()>>,
}

/// The writer's admission, ordering, and publication-rate policy, decided without I/O and with an
/// injected monotonic clock (`Instant`). `C` is a cut, `F` the final flush's reply and `T`
/// teardown custody, so the tables below drive it with plain values.
struct Slot<C, F, T> {
    /// The newest cut the writer has not taken; a newer one replaces it.
    latest: Option<C>,
    /// Whether the waiting cut bypasses `min_interval` and must flush immediately.
    urgent: bool,
    /// Whether the writer is writing a cut it took.
    in_flight: bool,
    /// Why the newest finished write failed, until a later write succeeds. Never retried.
    failure: Option<io::Error>,
    /// Minimum spacing between published ordinary cuts.
    min_interval: Duration,
    /// When the last ordinary cut was actually written to disk (byte-identical skips do not
    /// advance this).
    last_published: Option<Instant>,
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

/// Decision of [`Slot::next`]: run work now, wait for an event, or wait until the rate window's
/// deadline (while still waking immediately on a new cut, finish, or teardown).
enum Next<C, F, T> {
    Work(Work<C, F, T>),
    Wait,
    WaitUntil(Instant),
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
    #[cfg(test)]
    fn new() -> Self {
        Self::with_interval(Duration::ZERO)
    }

    fn with_interval(min_interval: Duration) -> Self {
        Self {
            latest: None,
            urgent: false,
            in_flight: false,
            failure: None,
            min_interval,
            last_published: None,
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

    /// Replace the waiting cut and mark it urgent so it bypasses `min_interval`.
    #[cfg(target_os = "macos")]
    fn publish_urgent(&mut self, cut: C) -> Result<Option<C>, C> {
        match self.phase {
            Phase::Open => {
                self.urgent = true;
                Ok(self.latest.replace(cut))
            }
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

    /// The writer's next work at `now` once its in-flight write is done. Ordinary cuts in
    /// `Phase::Open` wait until `last_published + min_interval` (leaving `latest` in place so
    /// newer cuts coalesce into it); `Finishing` and `TearingDown` bypass the bound and flush at
    /// once.
    fn next(&mut self, now: Instant) -> Next<C, F, T> {
        if self.in_flight {
            return Next::Wait;
        }
        match &self.phase {
            Phase::Open => {
                if self.latest.is_some() {
                    if !self.urgent
                        && !self.min_interval.is_zero()
                        && let Some(last) = self.last_published
                        && let Some(deadline) = last.checked_add(self.min_interval)
                        && now < deadline
                    {
                        return Next::WaitUntil(deadline);
                    }
                    if let Some(cut) = self.latest.take() {
                        self.urgent = false;
                        self.in_flight = true;
                        return Next::Work(Work::Write(cut));
                    }
                }
                return Next::Wait;
            }
            Phase::Finishing(_) => {
                if let Some(cut) = self.latest.take() {
                    self.urgent = false;
                    self.in_flight = true;
                    return Next::Work(Work::Write(cut));
                }
            }
            Phase::TearingDown { .. } | Phase::Retiring | Phase::Closed { .. } | Phase::Dead => {}
        }
        match std::mem::replace(&mut self.phase, Phase::Retiring) {
            Phase::TearingDown { custody, finish } => Next::Work(Work::Retire { custody, finish }),
            Phase::Finishing(reply) => {
                let result = self.failure.take().map_or(Ok(()), Err);
                self.phase = Phase::Closed { ok: result.is_ok() };
                Next::Work(Work::Finish { reply, result })
            }
            phase => {
                self.phase = phase;
                Next::Wait
            }
        }
    }

    /// The in-flight write ended at `now`. `Ok(true)` means bytes were published to disk and
    /// starts the next rate-limit window; `Ok(false)` means a byte-identical cut was skipped and
    /// does not consume the window. A failure is kept, not retried.
    fn written(&mut self, result: io::Result<bool>, now: Instant) {
        self.in_flight = false;
        match result {
            Ok(true) => {
                self.failure = None;
                self.last_published = Some(now);
            }
            Ok(false) => {
                self.failure = None;
            }
            Err(error) => {
                self.failure = Some(error);
            }
        }
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
        if cut.claims.capacity() > self.spare.claims.capacity()
            || cut.darwinbg.capacity() > self.spare.darwinbg.capacity()
        {
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
                slot: Slot::with_interval(MIN_PUBLISH_INTERVAL),
                spare: Cut::new(),
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

    /// Hand the current claims and active `PRIO_DARWIN_BG` lives to the writer without waiting on
    /// its I/O: they replace any cut still waiting. Refused once the final flush or teardown has
    /// begun.
    pub(super) fn publish(
        &self,
        claims: impl Iterator<Item = JobGroupClaim>,
        darwinbg: &[ProcessIdentity],
    ) -> io::Result<()> {
        let mut cut = std::mem::replace(&mut self.shared.lock().spare, Cut::new());
        cut.claims.extend(claims);
        cut.darwinbg.extend_from_slice(darwinbg);
        let mut state = self.shared.lock();
        match state.slot.publish(cut) {
            Ok(replaced) => {
                if let Some(mut replaced) = replaced {
                    if let Some(ack) = replaced.ack.take() {
                        // A superseded urgent cut's waiter is answered by the newer cut: move it.
                        if let Some(latest) = state.slot.latest.as_mut() {
                            latest.ack = Some(ack);
                            state.slot.urgent = true;
                        }
                    }
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

    /// Hand `claims` and `darwinbg` to the writer, bypassing `MIN_PUBLISH_INTERVAL`, and answer on
    /// the returned channel once the cut has been published on disk (05_gateway.md "What a crash
    /// leaves": append to `.groups` before setting `PRIO_DARWIN_BG`).
    #[cfg(target_os = "macos")]
    pub(super) fn publish_immediate(
        &self,
        claims: impl Iterator<Item = JobGroupClaim>,
        darwinbg: &[ProcessIdentity],
    ) -> tokio::sync::oneshot::Receiver<io::Result<()>> {
        let (reply, answer) = tokio::sync::oneshot::channel();
        let mut cut = std::mem::replace(&mut self.shared.lock().spare, Cut::new());
        cut.claims.extend(claims);
        cut.darwinbg.extend_from_slice(darwinbg);
        cut.ack = Some(reply);
        let mut state = self.shared.lock();
        match state.slot.publish_urgent(cut) {
            Ok(replaced) => {
                if let Some(mut replaced) = replaced {
                    if let Some(old_ack) = replaced.ack.take() {
                        let _ = old_ack.send(Ok(()));
                    }
                    replaced.clear();
                    state.recycle(replaced);
                }
                drop(state);
                self.shared.wake.notify_one();
            }
            Err(mut cut) => {
                let ack = cut.ack.take();
                cut.clear();
                state.recycle(cut);
                let error = self.refused(state.slot.describe());
                drop(state);
                if let Some(ack) = ack {
                    let _ = ack.send(Err(error));
                }
            }
        }
        answer
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
    mut recorder: Recorder,
    mut inherited: Vec<UnresolvedGroup>,
    lease: Option<PublicationLease>,
) {
    let _ended = Ended(shared);
    let mut state = shared.lock();
    loop {
        let work = match state.slot.next(Instant::now()) {
            Next::Work(work) => work,
            Next::Wait => {
                state = shared.wake.wait(state).expect("group ledger slot poisoned");
                continue;
            }
            Next::WaitUntil(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if !remaining.is_zero() {
                    (state, _) = shared
                        .wake
                        .wait_timeout(state, remaining)
                        .expect("group ledger slot poisoned");
                }
                continue;
            }
        };
        drop(state);
        match work {
            Work::Write(mut cut) => {
                let ack = cut.ack.take();
                let result = record(
                    &mut recorder,
                    cut.claims.iter().cloned(),
                    &cut.darwinbg,
                    &mut inherited,
                );
                let finished_at = Instant::now();
                if let Some(ack) = ack {
                    let answer = match &result {
                        Ok(_) => Ok(()),
                        Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
                    };
                    let _ = ack.send(answer);
                }
                cut.clear();
                state = shared.lock();
                state.slot.written(result, finished_at);
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
                let _ = record(
                    &mut recorder,
                    super::job_group_claims(&jobs),
                    &[],
                    &mut inherited,
                );
                let result =
                    super::retire_dropped_jobs(jobs, Some(&mut recorder), inherited, grace);
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

/// One ledger rewrite. A success carries the inherited groups still held into the next one and
/// reports whether bytes were written (`true`) or skipped as byte-identical (`false`).
fn record(
    recorder: &mut Recorder,
    claims: impl Iterator<Item = JobGroupClaim> + Clone,
    darwinbg: &[ProcessIdentity],
    inherited: &mut Vec<UnresolvedGroup>,
) -> io::Result<bool> {
    match recorder.record_with_darwinbg(claims, inherited, darwinbg) {
        Ok(recorded) => {
            for error in &recorded.uninspected {
                cowshed_os::eprintln!(
                    "cowshed: an inherited unresolved process group in {} could not be \
                 inspected and is carried as it was: {error}",
                    recorder.path().display()
                );
            }
            *inherited = recorded.carried;
            Ok(recorded.published)
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

    fn apply(slot: &mut Table, step: Step, now: Instant) -> Seen {
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
            Step::Next => match slot.next(now) {
                Next::Wait | Next::WaitUntil(_) => Seen::Idle,
                Next::Work(Work::Write(cut)) => Seen::Write(cut),
                Next::Work(Work::Finish { reply, result }) => {
                    Seen::Finish(reply, result.map_err(|error| error.to_string()))
                }
                Next::Work(Work::Retire { custody, finish }) => Seen::Retire(custody, finish),
            },
            Step::Written(failure) => {
                slot.written(
                    failure.map_or(Ok(true), |failure| Err(io::Error::other(failure))),
                    now,
                );
                Seen::Written
            }
            Step::Dead => Seen::Dead(slot.dead()),
            Step::ClosedOk => Seen::ClosedOk(slot.closed_ok()),
        }
    }

    fn run(table: &[(Step, Seen)]) {
        let mut slot = Table::new();
        let now = Instant::now();
        for (index, (step, expected)) in table.iter().enumerate() {
            assert_eq!(&apply(&mut slot, *step, now), expected, "step {index}");
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

    #[test]
    fn cuts_within_the_rate_bound_wait_and_coalesce_until_the_interval_elapses() {
        let origin = std::time::Instant::now();
        let at = |ms| origin + Duration::from_millis(ms);
        let mut slot = Table::with_interval(MIN_PUBLISH_INTERVAL);

        assert_eq!(slot.publish("a"), Ok(None));
        assert!(matches!(slot.next(at(0)), Next::Work(Work::Write("a"))));
        slot.written(Ok(true), at(0));
        assert!(matches!(slot.next(at(50)), Next::Wait));

        assert_eq!(slot.publish("b"), Ok(None));
        assert_eq!(slot.publish("c"), Ok(Some("b")));
        assert!(matches!(
            slot.next(at(500)),
            Next::WaitUntil(deadline) if deadline == at(1000)
        ));
        assert_eq!(slot.publish("d"), Ok(Some("c")));
        assert!(matches!(
            slot.next(at(800)),
            Next::WaitUntil(deadline) if deadline == at(1000)
        ));
        assert!(matches!(slot.next(at(1000)), Next::Work(Work::Write("d"))));
        slot.written(Ok(true), at(1000));
        assert!(matches!(slot.next(at(1000)), Next::Wait));

        // A skipped byte-identical cut (`Ok(false)`) did not publish and does not start a new
        // rate-limit window for a genuinely changed cut right after it.
        assert_eq!(slot.publish("e"), Ok(None));
        assert!(matches!(slot.next(at(2000)), Next::Work(Work::Write("e"))));
        slot.written(Ok(false), at(2000));
        assert_eq!(slot.publish("f"), Ok(None));
        assert!(matches!(slot.next(at(2050)), Next::Work(Work::Write("f"))));
    }

    #[test]
    fn finish_and_teardown_bypass_the_rate_bound_immediately() {
        let origin = std::time::Instant::now();
        let at = |ms| origin + Duration::from_millis(ms);

        let mut finishing = Table::with_interval(MIN_PUBLISH_INTERVAL);
        assert_eq!(finishing.publish("a"), Ok(None));
        assert!(matches!(
            finishing.next(at(0)),
            Next::Work(Work::Write("a"))
        ));
        finishing.written(Ok(true), at(0));
        assert_eq!(finishing.publish("b"), Ok(None));
        assert!(matches!(
            finishing.next(at(100)),
            Next::WaitUntil(deadline) if deadline == at(1000)
        ));
        assert_eq!(finishing.finish("final"), Ok(()));
        assert!(matches!(
            finishing.next(at(200)),
            Next::Work(Work::Write("b"))
        ));
        finishing.written(Ok(true), at(200));
        assert!(matches!(
            finishing.next(at(200)),
            Next::Work(Work::Finish {
                reply: "final",
                result: Ok(())
            })
        ));

        let mut tearing_down = Table::with_interval(MIN_PUBLISH_INTERVAL);
        assert_eq!(tearing_down.publish("a"), Ok(None));
        assert!(matches!(
            tearing_down.next(at(0)),
            Next::Work(Work::Write("a"))
        ));
        tearing_down.written(Ok(true), at(0));
        assert_eq!(tearing_down.publish("b"), Ok(None));
        assert!(matches!(
            tearing_down.next(at(100)),
            Next::WaitUntil(deadline) if deadline == at(1000)
        ));
        assert!(matches!(
            tearing_down.teardown("jobs", false),
            Handoff::Queued(Some("b"))
        ));
        assert!(matches!(
            tearing_down.next(at(200)),
            Next::Work(Work::Retire {
                custody: "jobs",
                finish: None
            })
        ));
    }

    /// Drives the real `GroupLedgerWriter` thread across a burst of 20 byte-identical cuts and a
    /// burst of 20 distinct cuts: identical cuts never trigger a second temp+rename rewrite,
    /// distinct cuts within 1 s coalesce to at most 2 rewrites (including final flush), and
    /// `finish` flushes the newest cut at once.
    #[tokio::test]
    async fn a_burst_of_cuts_renames_the_ledger_at_most_once_a_second_and_never_for_an_identical_one()
     {
        use crate::runtime::job_groups::GroupLeader;
        use std::os::unix::fs::MetadataExt as _;

        let root = crate::temp_root::TempRoot::new("group-ledger-burst");
        let ledger = root.join("supervisor.groups");
        let mut writer = GroupLedgerWriter::start(Recorder::new(ledger.clone()), Vec::new(), None)
            .expect("start group ledger writer");

        let sample_inode = |path: &Path| {
            std::fs::symlink_metadata(path)
                .ok()
                .map(|metadata| metadata.ino())
        };
        let record_inode = |path: &Path, last: &mut Option<u64>, rewrites: &mut usize| {
            if let Some(inode) = sample_inode(path)
                && *last != Some(inode)
            {
                *last = Some(inode);
                *rewrites += 1;
            }
        };

        // Burst 1: 20 byte-identical cuts over ~200 ms. Wait for the first write to land, then
        // pace the remaining 19 so an unthrottled writer would rewrite on every cut.
        let same = GroupLeader::from_parts(42, 1, 100).expect("valid group leader");
        writer
            .publish([JobGroupClaim::group(1, same)].into_iter(), &[])
            .expect("publish initial cut");
        let first_inode = loop {
            if let Some(inode) = sample_inode(&ledger) {
                break inode;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        };
        let mut last_inode = Some(first_inode);
        let mut identical_rewrites = 1_usize;
        for _ in 1..20 {
            writer
                .publish([JobGroupClaim::group(1, same)].into_iter(), &[])
                .expect("publish identical cut");
            tokio::time::sleep(Duration::from_millis(10)).await;
            record_inode(&ledger, &mut last_inode, &mut identical_rewrites);
        }
        assert!(
            identical_rewrites <= 1,
            "20 byte-identical cuts must cause at most 1 rewrite, observed {identical_rewrites}"
        );

        // Burst 2: 20 distinct cuts over ~500 ms, then finish immediately.
        let mut distinct_rewrites = 0_usize;
        for step in 0..20_u64 {
            let pgid = i32::try_from(100 + step).expect("pgid");
            let leader = GroupLeader::from_parts(pgid, 1 + step, 1000 + step).expect("leader");
            writer
                .publish([JobGroupClaim::group(10 + step, leader)].into_iter(), &[])
                .expect("publish distinct cut");
            tokio::time::sleep(Duration::from_millis(25)).await;
            record_inode(&ledger, &mut last_inode, &mut distinct_rewrites);
        }

        writer
            .finish()
            .await
            .expect("writer answers final flush")
            .expect("final flush succeeds");
        record_inode(&ledger, &mut last_inode, &mut distinct_rewrites);

        assert!(
            distinct_rewrites <= 2,
            "20 distinct cuts within 1 s plus final flush must cause at most 2 rewrites, observed {distinct_rewrites}"
        );
        let published: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&ledger).expect("read final ledger"))
                .expect("parse final ledger");
        let groups = published["groups"].as_array().expect("groups array");
        assert_eq!(
            groups.len(),
            1,
            "final ledger holds the last cut: {published}"
        );
        assert_eq!(groups[0]["jobId"].as_u64(), Some(29));
        assert_eq!(groups[0]["pgid"].as_i64(), Some(119));
        assert_eq!(groups[0]["leaderStart"].as_u64(), Some(1019));
    }
}
