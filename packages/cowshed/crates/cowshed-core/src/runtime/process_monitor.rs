//! One job's native observation writer (07_api.md, "Process-tree observations").
//!
//! The writer alone owns the kqueue, retained fold and event journal. Readers load one immutable
//! snapshot; a demand stream reads the retained journal without resampling or moving a CPU window.
//! A user event wakes the writer for control requests, never another reader of process events.
//! A sealed job drops its monitor: the writer, its thread, kqueue, fold and journal end with the
//! last clone, after any subscriber still attached has read to the end.
//!
//! The writer is also the job's sole retirement authority (11_shell.md, "Complete-group
//! termination"). Whole-job membership includes the held group; a subtree uses only immutable
//! ancestry in the retained fold. Both signal exact lives and join their kernel NOTE_EXIT.
//! Issued subtree capabilities bind a job, retained index and unguessable per-root nonce; exec,
//! exit and reparenting never move that capability to another life. No other registry takes part.

use std::collections::{HashMap, HashSet};
use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwap;
use tokio::sync::oneshot;

use super::job_attribution::{Attribution, AttributionSummary};
use super::job_groups::Birth;
use super::job_spans::{JobSpanPublisher, job_span_thread};
use super::process_events::{KqueueObserver, ObserveError};
use super::process_spans::{ProcessSpanProjection, RetainedProcessEvent};
use super::process_tree::{ProcessIdentity, TreeOwner};
use super::supervisor::{LeafCoverage, utc_now};
use crate::api::dto::JobInfo;
use crate::api::process::{
    JobContainment, JobProcessEvent, JobProcessRetirement, JobProcessTree, JobSubtreeAdmission,
    JobSubtreeHandle, JobSubtreeRefusal, JobSubtreeRetirement, OutlivedProcess, ProcessCoverageGap,
};
use crate::api::resources::{CpuTotals, ResourceUnitError};
use crate::error::{CowshedError, Result};

const DEFAULT_SAMPLE: Duration = Duration::from_secs(1);
const CONTROL_EVENT: usize = usize::MAX;
/// The least time SIGKILLed lives get to report their exits, whatever the TERM grace: the
/// bound `job_groups`' recovery gives its own KILL rounds.
const KILL_JOIN: Duration = Duration::from_secs(1);

type EventReply = oneshot::Sender<Option<Result<JobProcessEvent>>>;

enum Request {
    Running {
        reply: mpsc::SyncSender<Vec<ProcessIdentity>>,
    },
    Watch {
        pid: u32,
        reply: mpsc::SyncSender<Result<()>>,
    },
    Gap {
        reason: ProcessCoverageGap,
        reply: mpsc::SyncSender<Result<()>>,
    },
    Clock {
        origin: Instant,
    },
    Finish {
        reply: mpsc::SyncSender<Result<()>>,
    },
    Retire {
        birth: Birth,
        grace: Duration,
        /// The attempts completed when the caller asked: a later completion answers it.
        seen: u64,
        reply: mpsc::SyncSender<Result<JobProcessRetirement>>,
    },
    OwnsLive {
        peer: ProcessIdentity,
        reply: mpsc::SyncSender<Result<bool>>,
    },
    SubtreeHandle {
        peer: ProcessIdentity,
        reply: mpsc::SyncSender<Result<JobSubtreeAdmission>>,
    },
    RetireSubtree {
        handle: JobSubtreeHandle,
        grace: Duration,
        reply: mpsc::SyncSender<Result<JobSubtreeRetirement>>,
    },
    RetireRoot {
        root: ProcessIdentity,
        grace: Duration,
        reply: mpsc::SyncSender<Result<JobProcessRetirement>>,
    },
    LedgerChanges(Arc<tokio::sync::Notify>),
    Trace(Box<TraceWriter>),
    Open {
        every: Duration,
        reply: oneshot::Sender<Result<u64>>,
    },
    Next {
        id: u64,
        cursor: usize,
        reply: EventReply,
    },
    Close {
        id: u64,
    },
    Stop,
}

struct Subscriber {
    every: Duration,
    pending: Option<(usize, EventReply)>,
}

struct RetainedEntry {
    event: Result<JobProcessEvent>,
    identity: Option<ProcessIdentity>,
    parent: Option<ProcessIdentity>,
    at: SystemTime,
    /// The last entry of one emitted fold batch, retained for exact trace replay.
    batch_end: bool,
}

struct TraceWriter {
    publisher: JobSpanPublisher,
    projection: ProcessSpanProjection,
}

impl TraceWriter {
    fn record(&mut self, entries: &[RetainedEntry]) {
        let Self {
            publisher,
            projection,
        } = self;
        publisher.record_process(entries.iter().filter_map(|entry| {
            entry.event.as_ref().ok().map(|event| {
                projection.project(
                    RetainedProcessEvent {
                        event,
                        identity: entry.identity,
                        parent: entry.parent,
                    },
                    entry.at,
                )
            })
        }));
    }

    fn replay(&mut self, entries: &[RetainedEntry]) {
        for batch in entries.split_inclusive(|entry| entry.batch_end) {
            self.record(batch);
        }
    }
}

enum SnapshotTree {
    Job(Result<JobProcessTree>),
    Spare,
}
impl SnapshotTree {
    fn job(&self) -> Result<&JobProcessTree> {
        match self {
            Self::Job(tree) => tree.as_ref().map_err(Clone::clone),
            Self::Spare => Err(CowshedError::usage(
                "a shell spare's activation has no job process tree",
                "read the processes of an admitted job",
            )),
        }
    }
}
struct Snapshot {
    tree: SnapshotTree,
    attribution: std::result::Result<AttributionSummary, ResourceUnitError>,
    /// Every life the fold holds with no folded exit, by its kernel unique id. Shared with the
    /// ledger's claims, so a publication never copies it.
    running: Arc<[ProcessIdentity]>,
}

struct Driver {
    job_id: TreeOwner,
    wake: OwnedFd,
    requests: mpsc::Sender<Request>,
    snapshot: Arc<ArcSwap<Snapshot>>,
    /// Retirement attempts the writer completed; only the writer advances it.
    attempts: Arc<AtomicU64>,
    thread: Option<JoinHandle<()>>,
}

impl Driver {
    fn send(&self, request: Request) -> Result<()> {
        self.requests
            .send(request)
            .map_err(|_| unavailable(self.job_id))?;
        control(self.wake.as_raw_fd(), 0, libc::NOTE_TRIGGER).map_err(|error| {
            CowshedError::internal(format!(
                "cannot wake {}'s process observer: {error}",
                self.job_id
            ))
        })
    }

    fn acknowledged(
        &self,
        request: impl FnOnce(mpsc::SyncSender<Result<()>>) -> Request,
    ) -> Result<()> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.send(request(reply))?;
        answer.recv().map_err(|_| unavailable(self.job_id))?
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        if let Err(error) = self.send(Request::Stop) {
            cowshed_os::eprintln!("cowshed: stopping process observation: {error}");
        }
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            cowshed_os::eprintln!(
                "cowshed: {}'s process observation writer panicked",
                self.job_id
            );
        }
    }
}

/// The job record owns this monitor; clones do not own another writer or sampling window.
#[derive(Clone)]
pub struct ProcessMonitor {
    driver: Arc<Driver>,
}

impl std::fmt::Debug for ProcessMonitor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessMonitor")
            .field("job_id", &self.driver.job_id)
            .finish_non_exhaustive()
    }
}

impl ProcessMonitor {
    /// Prepare before the spawn. A failed source is a failed observation fence, not polling.
    pub fn prepare(job_id: TreeOwner) -> Result<Self> {
        let observer = KqueueObserver::prepare().map_err(observation_error)?;
        control(observer.as_raw_fd(), libc::EV_ADD | libc::EV_CLEAR, 0).map_err(|error| {
            CowshedError::internal(format!(
                "cannot prepare the process observer's control event: {error}"
            ))
        })?;
        let wake = observer.wake_handle().map_err(|error| {
            CowshedError::internal(format!(
                "cannot retain the process observer's wake descriptor: {error}"
            ))
        })?;
        let snapshot = Arc::new(ArcSwap::from_pointee(Snapshot {
            tree: match job_id {
                TreeOwner::Job(_) => SnapshotTree::Job(Err(CowshedError::conflict(
                    format!("{job_id} has no observed process yet"),
                    "read its processes once the command has started",
                ))),
                TreeOwner::Spare => SnapshotTree::Spare,
            },
            attribution: AttributionSummary::from_observed(std::iter::empty()),
            running: Arc::from([]),
        }));
        let (requests, receiver) = mpsc::channel();
        let published = Arc::clone(&snapshot);
        let attempts = Arc::new(AtomicU64::new(0));
        let completed = Arc::clone(&attempts);
        let thread = thread::Builder::new()
            .name(format!("cowshed-process-{}", job_id))
            .spawn(move || Writer::new(job_id, observer, published, completed).run(receiver))
            .map_err(|error| {
                CowshedError::internal(format!(
                    "cannot start the process observation writer: {error}"
                ))
            })?;
        Ok(Self {
            driver: Arc::new(Driver {
                job_id,
                wake,
                requests,
                snapshot,
                attempts,
                thread: Some(thread),
            }),
        })
    }

    /// Spawn a root held at the in-process leader gate until this monitor watches its exact life.
    /// The shell host uses the same native fold and subtree retirement as jobs, without watching
    /// itself (which must stay alive to finish teardown).
    pub fn spawn_root(
        &self,
        mut command: std::process::Command,
    ) -> io::Result<(std::process::Child, ProcessIdentity)> {
        use cowshed_os::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;
        let (gate, held) = super::process_fence::leader_gate()?;
        let hold = held.in_process(&gate);
        // SAFETY: the hold only closes descriptors and calls getpid/write/read before exec.
        unsafe {
            command.pre_exec(move || hold.hold());
        }
        thread::scope(|scope| {
            let spawning = scope.spawn(move || {
                let child = command.spawn_locked();
                drop(held);
                child
            });
            let mut root = None;
            let observed = gate.observe(|pid| {
                self.watch(pid).map_err(io::Error::other)?;
                root = self
                    .running_lives()
                    .iter()
                    .copied()
                    .find(|life| life.pid == pid);
                assert!(root.is_some(), "the held root was just watched");
                Ok(())
            });
            let spawned = spawning
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            match (observed, spawned) {
                (Ok(Some(_)), Ok(child)) => Ok((child, root.expect("the gate watched its root"))),
                (Ok(_), Err(error)) => Err(error),
                (Err(observation), Err(spawn)) => Err(io::Error::other(format!(
                    "cannot observe activation root: {observation}; held spawn failed: {spawn}"
                ))),
                (observed, Ok(mut child)) => {
                    let killed = child.kill();
                    let waited = cowshed_os::deadline::reap(
                        "activation root",
                        &mut child,
                        &cowshed_os::deadline::start(
                            "reap unfenced activation root",
                            cowshed_os::deadline::REAP,
                        ),
                        cowshed_os::deadline::Progress::Signalled {
                            signal: libc::SIGKILL,
                        },
                    );
                    if waited.is_err() {
                        cowshed_os::deadline::collect_later("activation root", child);
                    }
                    Err(io::Error::other(format!(
                        "activation started without its observation fence: {observed:?}; kill: {killed:?}; reap: {waited:?}"
                    )))
                }
            }
        })
    }

    /// Retire a locally observed activation root and its retained descendants, never the host's
    /// group. Reparenting and setsid do not change immutable ancestry in the fold.
    pub fn retire_root(
        &self,
        root: ProcessIdentity,
        grace: Duration,
    ) -> Result<JobProcessRetirement> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.driver
            .send(Request::RetireRoot { root, grace, reply })?;
        answer.recv().map_err(|_| unavailable(self.driver.job_id))?
    }
    /// Synchronous because the held child cannot exec until this registration is acknowledged.
    pub(crate) fn watch(&self, pid: u32) -> Result<()> {
        self.driver
            .acknowledged(|reply| Request::Watch { pid, reply })
    }

    pub(crate) fn gap(&self, reason: ProcessCoverageGap) -> Result<()> {
        self.driver
            .acknowledged(|reply| Request::Gap { reason, reply })
    }

    /// The job's clock started at `origin`, the instant its first owned process spawned and the
    /// origin of its samples' `wallUs`: usage windows open on it from then on. A refusal reaches
    /// readers as the observation's error.
    pub(crate) fn clock(&self, origin: Instant) -> Result<()> {
        self.driver.send(Request::Clock { origin })
    }

    /// Drain the job's accounting boundary. Still-live descendants remain watched, and their
    /// stream ends only when the last retained registration ends.
    pub(crate) fn finish(&self) -> Result<()> {
        self.driver.acknowledged(|reply| Request::Finish { reply })
    }

    /// The lives the observation saw born and not yet exit, in birth order: once the job ended,
    /// what of it outlived its root.
    pub(crate) fn running(&self) -> Result<Vec<ProcessIdentity>> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.driver.send(Request::Running { reply })?;
        answer.recv().map_err(|_| unavailable(self.driver.job_id))
    }

    /// End every process the job owns and join each one's exit (11_shell.md, "Complete-group
    /// termination"); blocking, so the caller runs it off its actor while it still holds the
    /// unreaped leader `birth` names. Owned: the held group's running members, read under that
    /// leader's proof, and every life the fold holds running, in the group or out of it. Each is
    /// sent SIGTERM; after `grace` the survivors are sent SIGSTOP and their children listed while
    /// they are stopped, then every owned life is sent SIGKILL. It answers only once the fold
    /// holds every owned life's kernel exit and the group read under the proof is empty. A
    /// stop's delivery proves no quiescence: a fork after it is the forking life's own
    /// `NOTE_FORK`, and its child is owned and killed like the others.
    ///
    /// The proof is [`JobContainment::Observed`]: its `lost` are the adoptions the observer saw
    /// it miss, empty when it saw none, which is no completeness claim. `outlived` names each life
    /// a terminal signal reached, with the last such signal.
    ///
    /// One attempt runs at a time on the sole writer. A call made while an attempt ran shares
    /// that attempt's outcome, sending nothing again; a later call after a refusal is an
    /// explicit retry with every registration and signal record retained; once retired, every
    /// call answers the same proof.
    pub fn retire(&self, birth: Birth, grace: Duration) -> Result<JobProcessRetirement> {
        let seen = self.driver.attempts.load(Ordering::Acquire);
        let (reply, answer) = mpsc::sync_channel(1);
        self.driver.send(Request::Retire {
            birth,
            grace,
            seen,
            reply,
        })?;
        answer.recv().map_err(|_| unavailable(self.driver.job_id))?
    }

    /// Authenticate a peer without issuing a capability, through the same native admission
    /// fence used below. The actor uses this before a peer can spend a handle for this job.
    pub(crate) fn owns_live(&self, peer: ProcessIdentity) -> Result<bool> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.driver.send(Request::OwnsLive { peer, reply })?;
        answer.recv().map_err(|_| unavailable(self.driver.job_id))?
    }

    /// Authenticate a live peer through the sole writer and issue a capability for its retained
    /// birth subtree. A pid by itself is never sufficient; repeated admission returns the same
    /// capability, which remains valid after this root execs or exits.
    pub fn subtree_handle(&self, peer: ProcessIdentity) -> Result<JobSubtreeAdmission> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.driver.send(Request::SubtreeHandle { peer, reply })?;
        answer.recv().map_err(|_| unavailable(self.driver.job_id))?
    }

    /// Retire only this issued root and its immutable descendants, with the whole-job algorithm
    /// but without a group read. First use of an already-ended subtree returns an empty receipt.
    /// Success consumes the capability; a replay is SpentHandle, while operational errors leave
    /// it retryable. Prior signals remain available to the job's eventual terminal receipt.
    pub fn retire_subtree(
        &self,
        handle: JobSubtreeHandle,
        grace: Duration,
    ) -> Result<JobSubtreeRetirement> {
        let (reply, answer) = mpsc::sync_channel(1);
        self.driver.send(Request::RetireSubtree {
            handle,
            grace,
            reply,
        })?;
        answer.recv().map_err(|_| unavailable(self.driver.job_id))?
    }

    /// Attach the existing supervisor writer. The source replays its retained events,
    /// with their original identities and times, before projecting any future event.
    pub(crate) fn trace(&self, publisher: JobSpanPublisher, info: &JobInfo) -> Result<()> {
        self.driver.send(Request::Trace(Box::new(TraceWriter {
            publisher,
            projection: ProcessSpanProjection::new(
                info.trace.trace_id.as_str(),
                job_span_thread(&info.repo_id, &info.workspace_incarnation, info.job_id),
            ),
        })))
    }

    /// Wake the existing ledger owner after membership changes; a wake carries no ownership.
    pub(crate) fn ledger_changes(&self, changed: Arc<tokio::sync::Notify>) -> Result<()> {
        self.driver.send(Request::LedgerChanges(changed))
    }
    /// One publication load. A read neither touches the writer's fold nor samples the kernel.
    pub fn tree(&self) -> Result<JobProcessTree> {
        self.driver.snapshot.load().tree.job().cloned()
    }

    /// One publication load, answered even while a retirement occupies the writer: every life
    /// the fold holds with no folded exit, in the group or out of it, each by its pid and the
    /// kernel's unique id for that life, which no later process given the pid carries. Group
    /// members the fold never adopted are not here; a retirement's membership read adopts them.
    pub fn running_lives(&self) -> Arc<[ProcessIdentity]> {
        Arc::clone(&self.driver.snapshot.load().running)
    }

    /// One publication load and the canonical B13 comparison; no resample or CPU-window move.
    pub(crate) fn attribution(
        &self,
        total: CpuTotals,
        required: LeafCoverage,
    ) -> Result<Attribution> {
        let snapshot = self.driver.snapshot.load();
        let tree = snapshot.tree.job()?;
        snapshot
            .attribution
            .as_ref()
            .map_err(|error| {
                CowshedError::internal(format!("cannot summarize observed process CPU: {error}"))
            })?
            .attribute(total, required, tree.coverage)
            .map_err(|error| {
                CowshedError::internal(format!("cannot attribute observed process CPU: {error}"))
            })
    }

    pub async fn subscribe(
        &self,
        every: Duration,
    ) -> Result<super::process_stream::JobProcessStream> {
        if every.is_zero() || Instant::now().checked_add(every).is_none() {
            return Err(CowshedError::usage(
                "process sampling interval is outside the clock's range",
                "use a positive, shorter everyMs",
            ));
        }
        let (reply, answer) = oneshot::channel();
        self.driver.send(Request::Open { every, reply })?;
        let id = answer
            .await
            .map_err(|_| unavailable(self.driver.job_id))??;
        let mut subscription = NativeSubscription {
            driver: Arc::clone(&self.driver),
            id,
            cursor: 0,
            ended: false,
        };
        let (events, received) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    biased;
                    () = events.closed() => return,
                    next = subscription.next() => next,
                };
                let Some(event) = next else { return };
                if events.send(event).await.is_err() {
                    return;
                }
            }
        });
        Ok(super::process_stream::JobProcessStream::new(received))
    }
}

/// One private journal position, transported by the canonical worker-stream demand row.
/// There is no public cursor operation and a dropped reader never ends the command.
struct NativeSubscription {
    driver: Arc<Driver>,
    id: u64,
    cursor: usize,
    ended: bool,
}

impl std::fmt::Debug for NativeSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("JobProcessStream")
            .field("cursor", &self.cursor)
            .finish_non_exhaustive()
    }
}

impl NativeSubscription {
    pub async fn next(&mut self) -> Option<Result<JobProcessEvent>> {
        if self.ended {
            return None;
        }
        let (reply, answer) = oneshot::channel();
        if let Err(error) = self.driver.send(Request::Next {
            id: self.id,
            cursor: self.cursor,
            reply,
        }) {
            self.ended = true;
            return Some(Err(error));
        }
        match answer.await {
            Ok(Some(event)) => {
                self.cursor += 1;
                Some(event)
            }
            Ok(None) => {
                self.ended = true;
                None
            }
            Err(_) => {
                self.ended = true;
                Some(Err(unavailable(self.driver.job_id)))
            }
        }
    }
}

impl Drop for NativeSubscription {
    fn drop(&mut self) {
        if let Err(error) = self.driver.send(Request::Close { id: self.id }) {
            cowshed_os::eprintln!("cowshed: closing process subscription: {error}");
        }
    }
}

#[derive(Clone, Copy)]
enum RetirementScope<'a> {
    Job(&'a Birth),
    Subtree(ProcessIdentity),
}

struct TerminalSignal {
    signal: libc::c_int,
    /// A successful subtree receipt reported this row; retain it for the whole-job receipt.
    subtree_reported: bool,
}

struct Writer {
    observer: KqueueObserver,
    snapshot: Arc<ArcSwap<Snapshot>>,
    journal: Vec<RetainedEntry>,
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    next_sample: Instant,
    /// The job-accounting boundary is complete. Live registrations still sample.
    finished: bool,
    /// `finished` and no retained registration remains. Readers then see the end.
    ended: bool,
    trace: Option<Box<TraceWriter>>,
    job_id: TreeOwner,
    /// Shared with the callers; advanced once per completed retirement attempt.
    attempts: Arc<AtomicU64>,
    /// The leader and outcome of the latest retirement attempt.
    retirement: Option<(Birth, Result<JobProcessRetirement>)>,
    /// Last terminal signal per life, retained until whole-job retirement. Subtree receipt
    /// delivery never erases the job's eventual terminal provenance.
    signalled: HashMap<ProcessIdentity, TerminalSignal>,
    ledger_changes: Option<Arc<tokio::sync::Notify>>,
}

impl Writer {
    fn new(
        job_id: TreeOwner,
        observer: KqueueObserver,
        snapshot: Arc<ArcSwap<Snapshot>>,
        attempts: Arc<AtomicU64>,
    ) -> Self {
        Self {
            observer,
            snapshot,
            journal: Vec::new(),
            subscribers: HashMap::new(),
            next_id: 1,
            next_sample: Instant::now() + DEFAULT_SAMPLE,
            finished: false,
            ended: false,
            trace: None,
            job_id,
            attempts,
            retirement: None,
            signalled: HashMap::new(),
            ledger_changes: None,
        }
    }

    fn publish(&mut self, outcome: Result<()>) {
        let at = SystemTime::now();
        let first = self.journal.len();
        for (event, identity, parent) in self.observer.drain_retained_events() {
            self.journal.push(RetainedEntry {
                event: Ok(event),
                identity,
                parent,
                at,
                batch_end: false,
            });
        }
        if self.journal.len() > first {
            self.journal
                .last_mut()
                .expect("the batch emitted an entry")
                .batch_end = true;
        }
        if let Some(trace) = &mut self.trace {
            trace.record(&self.journal[first..]);
        }
        let tree = match self.job_id {
            TreeOwner::Job(job_id) => SnapshotTree::Job(
                outcome
                    .clone()
                    .and_then(|()| utc_now().map(|at| self.observer.tree(job_id, at))),
            ),
            TreeOwner::Spare => SnapshotTree::Spare,
        };
        let error = match &tree {
            SnapshotTree::Job(result) => result.as_ref().err(),
            SnapshotTree::Spare => outcome.as_ref().err(),
        };
        if let Some(error) = error {
            self.journal.push(RetainedEntry {
                event: Err(error.clone()),
                identity: None,
                parent: None,
                at,
                batch_end: true,
            });
        }
        let previous = self.snapshot.load();
        let membership_changed = !previous
            .running
            .iter()
            .copied()
            .eq(self.observer.fold().running());
        // An unchanged membership keeps the published lives the ledger may still be sharing.
        let running = if membership_changed {
            self.observer.fold().running().collect()
        } else {
            Arc::clone(&previous.running)
        };
        drop(previous);
        self.snapshot.store(Arc::new(Snapshot {
            tree,
            attribution: self.observer.attribution_summary(),
            running,
        }));
        if membership_changed && let Some(changed) = &self.ledger_changes {
            changed.notify_one();
        }
        if self.finished {
            self.ended = !self.observer.watching();
        }
        for subscriber in self.subscribers.values_mut() {
            if let Some((cursor, _)) = &subscriber.pending
                && (*cursor < self.journal.len() || self.ended)
            {
                let (cursor, reply) = subscriber
                    .pending
                    .take()
                    .expect("a pending demand was checked");
                let _ = reply.send(self.journal.get(cursor).map(|entry| entry.event.clone()));
            }
        }
    }

    fn run(mut self, receiver: mpsc::Receiver<Request>) {
        loop {
            if let Err(error) = self.observer.drain() {
                self.publish(Err(observation_error(error)));
            } else if self.observer.has_events()
                || (self.finished && !self.observer.watching() && !self.ended)
            {
                self.publish(Ok(()));
            }
            while let Ok(request) = receiver.try_recv() {
                match request {
                    Request::Running { reply } => {
                        let _ = reply.send(
                            self.observer
                                .fold()
                                .nodes()
                                .filter(|node| node.exit.is_none())
                                .map(|node| node.identity)
                                .collect(),
                        );
                    }
                    Request::Watch { pid, reply } => {
                        let result = self.observer.watch_root(pid).map_err(observation_error);
                        self.publish(result.clone());
                        let _ = reply.send(result);
                    }
                    Request::Gap { reason, reply } => {
                        let result = self.observer.gap(reason).map_err(observation_error);
                        self.publish(result.clone());
                        let _ = reply.send(result);
                    }
                    Request::Clock { origin } => {
                        // Starting the clock changes no record; only its refusal is news.
                        if let Err(error) = self.observer.clock(origin) {
                            self.publish(Err(observation_error(error)));
                        }
                    }
                    Request::Finish { reply } => {
                        self.finished = true;
                        let result = self.observer.drain().map_err(observation_error);
                        self.publish(result.clone());
                        let _ = reply.send(result);
                    }
                    Request::Retire {
                        birth,
                        grace,
                        seen,
                        reply,
                    } => {
                        let _ = reply.send(self.answer_retire(birth, grace, seen));
                    }
                    Request::OwnsLive { peer, reply } => {
                        let _ = reply.send(self.owns_live(peer));
                    }
                    Request::SubtreeHandle { peer, reply } => {
                        let _ = reply.send(self.subtree_handle(peer));
                    }
                    Request::RetireSubtree {
                        handle,
                        grace,
                        reply,
                    } => {
                        let _ = reply.send(self.retire_subtree(handle, grace));
                    }
                    Request::RetireRoot { root, grace, reply } => {
                        let outcome = if self.observer.fold().holds(root) {
                            self.retire(RetirementScope::Subtree(root), grace)
                        } else {
                            Err(CowshedError::conflict(
                                format!(
                                    "{} never observed activation root {}",
                                    self.job_id, root.pid
                                ),
                                "retire only a root spawned through this observation fence",
                            ))
                        };
                        let _ = reply.send(outcome);
                    }
                    Request::LedgerChanges(changed) => {
                        changed.notify_one();
                        self.ledger_changes = Some(changed);
                    }
                    Request::Trace(mut trace) => {
                        trace.replay(&self.journal);
                        self.trace = Some(trace);
                    }
                    Request::Open { every, reply } => match self.next_id.checked_add(1) {
                        Some(next) => {
                            let id = self.next_id;
                            self.next_id = next;
                            self.subscribers.insert(
                                id,
                                Subscriber {
                                    every,
                                    pending: None,
                                },
                            );
                            let _ = reply.send(Ok(id));
                        }
                        None => {
                            let _ = reply.send(Err(CowshedError::internal(
                                "process subscription identity exhausted",
                            )));
                        }
                    },
                    Request::Next { id, cursor, reply } => {
                        if cursor < self.journal.len() || self.ended {
                            let _ = reply
                                .send(self.journal.get(cursor).map(|entry| entry.event.clone()));
                        } else if let Some(subscriber) = self.subscribers.get_mut(&id) {
                            subscriber.pending = Some((cursor, reply));
                        } else {
                            let _ = reply.send(Some(Err(CowshedError::internal(
                                "process subscription is no longer registered",
                            ))));
                        }
                    }
                    Request::Close { id } => {
                        self.subscribers.remove(&id);
                    }
                    Request::Stop => return,
                }
            }
            if !self.ended && Instant::now() >= self.next_sample {
                if self.observer.watching() {
                    let result = self
                        .observer
                        .sample(Instant::now())
                        .map_err(observation_error);
                    self.publish(result);
                }
                let every = self
                    .subscribers
                    .values()
                    .map(|subscriber| subscriber.every)
                    .min()
                    .unwrap_or(DEFAULT_SAMPLE)
                    .min(DEFAULT_SAMPLE);
                self.next_sample = Instant::now() + every;
            }
            let timeout = if self.ended {
                -1
            } else {
                i32::try_from(
                    self.next_sample
                        .saturating_duration_since(Instant::now())
                        .as_millis(),
                )
                .unwrap_or(i32::MAX)
            };
            let mut descriptor = libc::pollfd {
                fd: self.observer.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one writable poll descriptor; its kqueue is owned for this whole call.
            if unsafe { libc::poll(&raw mut descriptor, 1, timeout) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    self.ended = true;
                    self.publish(Err(CowshedError::internal(format!(
                        "cannot wait for process events: {error}"
                    ))));
                    return;
                }
            }
        }
    }

    fn owns_live(&mut self, peer: ProcessIdentity) -> Result<bool> {
        let drained = self.observer.drain_custody();
        self.observed(drained)?;
        if !self.observer.fold().holds(peer) {
            // The peer may have connected before its parent's fork event was dequeued. Use
            // only our existing observer and retained parents, never a group or process census.
            let parents: Vec<_> = self.observer.fold().running().collect();
            for parent in parents {
                let adopted = self
                    .observer
                    .descendants(parent)
                    .map_err(|error| refused(self.job_id, error))?;
                self.observed(adopted)?;
            }
            let drained = self.observer.drain_custody();
            self.observed(drained)?;
        }
        self.observer.retained_live(peer).map_err(observation_error)
    }

    fn subtree_handle(&mut self, peer: ProcessIdentity) -> Result<JobSubtreeAdmission> {
        let TreeOwner::Job(job_id) = self.job_id else {
            return Ok(JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::NotMember,
            });
        };
        if !self.owns_live(peer)? {
            // Only the observation that held the peer can say its job ended. To any other the
            // peer is no member, so the supervisor asks on until the owner answers.
            return Ok(JobSubtreeAdmission::Refused {
                reason: if self.ended && self.observer.fold().holds(peer) {
                    JobSubtreeRefusal::JobEnded
                } else {
                    JobSubtreeRefusal::NotMember
                },
            });
        }
        let index = self
            .observer
            .fold()
            .index(peer)
            .expect("an authenticated retained life");
        if self.observer.fold().subtree_spent(index) {
            return Ok(JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::SpentHandle,
            });
        }
        let wire_index = u32::try_from(index)
            .map_err(|_| CowshedError::internal("subtree process index exceeds u32"))?;
        if self.observer.fold().subtree_nonce(index).is_none() {
            let mut random = [0_u8; 16];
            getrandom::fill(&mut random).map_err(|error| {
                CowshedError::internal(format!(
                    "cannot issue a process subtree capability: {error}"
                ))
            })?;
            self.observer
                .fold_mut()
                .issue_subtree(index, format!("{:032x}", u128::from_ne_bytes(random)));
        }
        Ok(JobSubtreeAdmission::Admitted {
            handle: JobSubtreeHandle {
                job_id,
                index: wire_index,
                incarnation: self
                    .observer
                    .fold()
                    .subtree_nonce(index)
                    .expect("the authenticated root was issued a nonce")
                    .to_owned(),
            },
        })
    }

    fn retire_subtree(
        &mut self,
        handle: JobSubtreeHandle,
        grace: Duration,
    ) -> Result<JobSubtreeRetirement> {
        let root = usize::try_from(handle.index).ok().and_then(|index| {
            (TreeOwner::Job(handle.job_id) == self.job_id
                && self.observer.fold().subtree_nonce(index) == Some(handle.incarnation.as_str()))
            .then(|| self.observer.fold().node_at(index))
            .flatten()
            .map(|node| (index, node.identity))
        });
        let Some((index, root)) = root else {
            return Ok(JobSubtreeRetirement::Refused {
                reason: JobSubtreeRefusal::ForeignHandle,
            });
        };
        if self.observer.fold().subtree_spent(index) {
            return Ok(JobSubtreeRetirement::Refused {
                reason: JobSubtreeRefusal::SpentHandle,
            });
        }
        let retirement = self.retire(RetirementScope::Subtree(root), grace)?;
        self.observer.fold_mut().spend_subtree(index);
        Ok(JobSubtreeRetirement::Retired { retirement })
    }

    /// One retirement request. A job retired answers its proof again; a request made while an
    /// attempt for the same leader ran shares that attempt's outcome; anything else is an attempt.
    fn answer_retire(
        &mut self,
        birth: Birth,
        grace: Duration,
        seen: u64,
    ) -> Result<JobProcessRetirement> {
        let attempts = self.attempts.load(Ordering::Acquire);
        match &self.retirement {
            Some((retired, Ok(proof))) if *retired == birth => return Ok(proof.clone()),
            Some((retired, Ok(_))) => {
                return Err(CowshedError::conflict(
                    format!(
                        "{}'s processes were retired under leader {}, not {}",
                        self.job_id,
                        retired.pid(),
                        birth.pid()
                    ),
                    "retire a job under the leader its own process group was started with",
                ));
            }
            Some((asked, outcome)) if seen < attempts && *asked == birth => return outcome.clone(),
            _ => {}
        }
        let outcome = self.retire(RetirementScope::Job(&birth), grace);
        self.retirement = Some((birth, outcome.clone()));
        self.attempts.store(attempts + 1, Ordering::Release);
        outcome
    }

    fn retire(
        &mut self,
        scope: RetirementScope<'_>,
        grace: Duration,
    ) -> Result<JobProcessRetirement> {
        // Fold every queued observation first: a life whose exit is already queued is joined,
        // never signalled.
        let drained = self.observer.drain_custody();
        self.observed(drained)?;
        for process in self.owned(scope)? {
            self.signal(process, libc::SIGTERM)?;
        }
        if self.join(scope, grace, None)?.is_empty() {
            return Ok(self.retired(scope));
        }
        self.freeze(scope, grace.max(KILL_JOIN))?;
        let mut killed = HashSet::new();
        let survivors = self.join(scope, grace.max(KILL_JOIN), Some(&mut killed))?;
        if survivors.is_empty() {
            return Ok(self.retired(scope));
        }
        Err(CowshedError::conflict(
            format!(
                "{}'s processes {:?} still run {:?} after SIGKILL",
                self.job_id,
                survivors.iter().map(|life| life.pid).collect::<Vec<_>>(),
                grace.max(KILL_JOIN)
            ),
            "they stay owned and watched; retry the retirement",
        ))
    }

    /// The selected membership now; subtree selection never expands to the job's group.
    fn owned(&mut self, scope: RetirementScope<'_>) -> Result<Vec<ProcessIdentity>> {
        let owned = match scope {
            RetirementScope::Job(birth) => self.observer.owned(birth),
            RetirementScope::Subtree(root) => self.observer.subtree(root),
        };
        if self.observer.has_events() {
            self.publish(Ok(()));
        }
        owned.map_err(|error| refused(self.job_id, error))
    }

    /// Signal one owned life; a terminal signal that reached it enters the receipt.
    fn signal(&mut self, process: ProcessIdentity, signal: libc::c_int) -> Result<bool> {
        let reached = self
            .observer
            .signal(process, signal)
            .map_err(|error| refused(self.job_id, error))?;
        if reached && signal != libc::SIGSTOP {
            self.signalled.insert(
                process,
                TerminalSignal {
                    signal,
                    subtree_reported: false,
                },
            );
        }
        Ok(reached)
    }

    /// Wait up to within for the selected lives' native exits, refreshing membership after
    /// every event batch. With killed, each running life is sent SIGKILL once. Only whole-job
    /// scope reads a group; an empty subtree means its retained exits were all joined.
    fn join(
        &mut self,
        scope: RetirementScope<'_>,
        within: Duration,
        mut killed: Option<&mut HashSet<ProcessIdentity>>,
    ) -> Result<Vec<ProcessIdentity>> {
        let deadline = deadline(within)?;
        loop {
            let owned = self.owned(scope)?;
            if let Some(killed) = &mut killed {
                for &process in &owned {
                    if killed.insert(process) {
                        self.signal(process, libc::SIGKILL)?;
                    }
                }
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if owned.is_empty() || left.is_zero() {
                return Ok(owned);
            }
            let waited = self.observer.wait_custody(left);
            self.observed(waited)?;
        }
    }

    /// SIGSTOP every owned life, then fold the forks queued meanwhile and list each stopped
    /// life's children while it still exists, until no new life is found. A stop is posted, not
    /// proven taken: a life may still fork, and that fork is its own registered event. Lives
    /// still appearing after `within` are refused by name, never taken as quiescent.
    fn freeze(&mut self, scope: RetirementScope<'_>, within: Duration) -> Result<()> {
        let deadline = deadline(within)?;
        let mut frozen = HashSet::new();
        loop {
            let fresh: Vec<_> = self
                .owned(scope)?
                .into_iter()
                .filter(|&process| frozen.insert(process))
                .collect();
            if fresh.is_empty() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(CowshedError::conflict(
                    format!(
                        "{}'s processes {:?} were still appearing {within:?} after its \
                         survivors were sent SIGSTOP: a posted stop proves no quiescence",
                        self.job_id,
                        fresh.iter().map(|life| life.pid).collect::<Vec<_>>(),
                    ),
                    "they stay owned and watched; retry the retirement",
                ));
            }
            for &process in &fresh {
                self.signal(process, libc::SIGSTOP)?;
            }
            let drained = self.observer.drain_custody();
            self.observed(drained)?;
            for process in fresh {
                let adopted = self
                    .observer
                    .descendants(process)
                    .map_err(|error| refused(self.job_id, error))?;
                self.observed(adopted)?;
            }
        }
    }

    /// Subtree delivery marks only its rows; whole-job delivery drains all signal provenance.
    fn retired(&mut self, scope: RetirementScope<'_>) -> JobProcessRetirement {
        let lives = self.observer.fold();
        let mut outlived: Vec<_> = self
            .signalled
            .iter_mut()
            .filter_map(|(&process, sent)| {
                if let RetirementScope::Subtree(root) = scope {
                    if sent.subtree_reported || !lives.in_subtree(root, process) {
                        return None;
                    }
                    sent.subtree_reported = true;
                }
                let index = lives.index(process);
                let node = index.and_then(|index| lives.node_at(index));
                Some(OutlivedProcess {
                    index: index.and_then(|index| u32::try_from(index).ok()),
                    pid: process.pid,
                    image: node
                        .filter(|node| node.image_known)
                        .map(|node| node.image.program.to_string()),
                    signal: sent.signal,
                })
            })
            .collect();
        outlived.sort_unstable_by_key(|outlived| (outlived.index, outlived.pid));
        if matches!(scope, RetirementScope::Job(_)) {
            self.signalled.clear();
        }
        JobProcessRetirement {
            containment: JobContainment::Observed {
                lost: self.observer.missed().to_vec(),
            },
            outlived,
            outlived_incomplete: false,
        }
    }

    /// Publish what an observer step folded, and its failure. The events were folded either
    /// way; custody-only steps still refuse every failed identity, child-list, watch or signal read.
    fn observed(&mut self, result: std::result::Result<(), ObserveError>) -> Result<()> {
        let result = result.map_err(|error| refused(self.job_id, error));
        if result.is_err() || self.observer.has_events() {
            self.publish(result.clone());
        }
        result
    }
}

/// The instant `within` from now on the retirement's clock.
fn deadline(within: Duration) -> Result<Instant> {
    Instant::now().checked_add(within).ok_or_else(|| {
        CowshedError::usage(
            "the retirement grace is outside the clock's range",
            "use a shorter grace",
        )
    })
}

fn observation_error(error: ObserveError) -> CowshedError {
    CowshedError::environment_missing(
        format!("process observation failed: {error}"),
        "inspect the retained coverage gap and the native call's error",
    )
}

fn unavailable(job_id: TreeOwner) -> CowshedError {
    CowshedError::internal(format!(
        "{}'s process observation writer is unavailable",
        job_id
    ))
}

/// A retirement step the kernel refused. Nothing is presumed ended: every registration, the
/// fold and the signal record stay for an explicit retry.
fn refused(job_id: TreeOwner, error: ObserveError) -> CowshedError {
    CowshedError::environment_missing(
        format!("{}'s processes cannot be retired: {error}", job_id),
        "its processes stay owned and watched; retry the retirement once the cause is resolved",
    )
}

fn control(queue: i32, flags: u16, fflags: u32) -> io::Result<()> {
    let change = libc::kevent {
        ident: CONTROL_EVENT,
        filter: libc::EVFILT_USER,
        flags,
        fflags,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    // SAFETY: one readable change, no returned events; the queue is owned by this caller.
    if unsafe { libc::kevent(queue, &change, 1, std::ptr::null_mut(), 0, std::ptr::null()) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::dto::{CommandArg, JobId, UtcTimestamp};
    use crate::api::process::JobProcessSample;
    use cowshed_os::test_backstop::ChildBackstop as _;

    #[test]
    fn a_spare_activation_root_is_watched_and_retired_without_a_job() {
        let monitor = ProcessMonitor::prepare(TreeOwner::Spare).unwrap();
        let mut command = std::process::Command::new("/bin/cat");
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null());
        let (mut child, root) = monitor.spawn_root(command).unwrap();
        assert!(monitor.running_lives().contains(&root));
        assert!(
            monitor
                .tree()
                .unwrap_err()
                .to_string()
                .contains("no job process tree")
        );
        assert_eq!(
            monitor.subtree_handle(root).unwrap(),
            JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::NotMember
            }
        );
        let retirement = monitor.retire_root(root, Duration::ZERO).unwrap();
        assert!(matches!(
            retirement.containment,
            JobContainment::Observed { .. }
        ));
        assert!(
            retirement
                .outlived
                .iter()
                .any(|process| process.pid == root.pid)
        );
        assert!(!child.wait_bounded().unwrap().success());
        assert!(monitor.running_lives().is_empty());
    }
    use crate::api::process::ProcessCoverage;
    use crate::runtime::process_spans::read_process_segment;
    use crate::runtime::process_tree::BirthToken;
    use crate::temp_root::TempRoot;

    /// Installed while the leader is still behind the pre-exec fence, before any assertion or
    /// workload fork. The leader remains our unreaped child until this guard kills its group.
    /// Cleanup needs neither a process census nor a pid inferred after an observation failure.
    struct HeldSubtreeGroup {
        pid: libc::pid_t,
        leader: Option<std::process::Child>,
    }

    impl Drop for HeldSubtreeGroup {
        fn drop(&mut self) {
            // SAFETY: this test established the group before the pre-exec fence and still holds
            // its unreaped leader. Every fixture descendant deliberately stays in this group.
            if unsafe { libc::killpg(self.pid, libc::SIGKILL) } != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    cowshed_os::eprintln!(
                        "cannot clean up subtree fixture group {}: {error}",
                        self.pid
                    );
                }
            }
            if let Some(leader) = &mut self.leader
                && let Err(error) = leader.wait_bounded()
            {
                cowshed_os::eprintln!("cannot reap subtree fixture leader {}: {error}", self.pid);
            }
        }
    }

    struct SubtreeFixture {
        monitor: ProcessMonitor,
        group: HeldSubtreeGroup,
        input: std::process::ChildStdin,
        lines: std::io::Lines<std::io::BufReader<std::process::ChildStdout>>,
        root: ProcessIdentity,
        child: ProcessIdentity,
        sibling: ProcessIdentity,
    }

    fn native_life(pid: u32) -> ProcessIdentity {
        let record =
            crate::runtime::job_groups::process_record(libc::pid_t::try_from(pid).unwrap())
                .unwrap()
                .expect("a fixture life still runs");
        ProcessIdentity {
            pid,
            birth: BirthToken(record.unique_id),
        }
    }

    impl SubtreeFixture {
        fn start() -> Self {
            Self::start_with_churn(false)
        }

        fn start_with_churn(churn: bool) -> Self {
            use cowshed_os::fork_lock::Spawn as _;
            use std::io::{BufRead as _, Write as _};
            use std::os::unix::process::CommandExt as _;
            use std::process::{Command, Stdio};

            let script = r#"
import os, signal, sys
root_read, root_write = os.pipe()
root = os.fork()
if root == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    child_read, child_write = os.pipe()
    child = os.fork()
    if child == 0:
        os.read(child_read, 1)
        os._exit(0)
    os.write(1, b"root %d %d\n" % (os.getpid(), child))
    command = os.read(root_read, 1)
    if command == b"e":
        os.execv(sys.executable, [sys.executable, "-c", "import os, signal; signal.signal(signal.SIGTERM, signal.SIG_IGN); os.write(1, b'exec\\n'); signal.pause()"])
    if command == b"n":
        os.write(child_write, b"x")
        os.waitpid(child, 0)
    os._exit(0)
churn_read, churn_write = os.pipe()
sibling = os.fork()
if sibling == 0:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    os.write(1, b"sibling %d\n" % os.getpid())
    if os.environ["COWSHED_TEST_SUBTREE_CHURN"] == "1":
        os.read(churn_read, 1)
        for _ in range(64):
            child = os.fork()
            if child == 0:
                os.execv("/usr/bin/true", ["true"])
            _, status = os.waitpid(child, 0)
            if status != 0:
                os._exit(2)
        os.write(1, b"churned 64\n")
    while True:
        signal.pause()
for line in sys.stdin:
    command = line.strip().encode()
    if command == b"c":
        os.write(1, b"churning\n")
        os.write(churn_write, b"x")
        continue
    os.write(root_write, command)
    if command in (b"x", b"n"):
        os.waitpid(root, 0)
        os.write(1, b"exited\n")
"#;
            let monitor =
                ProcessMonitor::prepare(TreeOwner::Job(JobId::new(401).unwrap())).unwrap();
            let (ready_read, ready_write) = std::io::pipe().unwrap();
            let (gate_read, mut gate_write) = std::io::pipe().unwrap();
            let mut command = Command::new("/bin/bash");
            command
                .args(["-c", "exec python3 -c \"$1\"", "subtree-fixture", script])
                .env("COWSHED_TEST_SUBTREE_CHURN", if churn { "1" } else { "0" })
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .process_group(0);
            let (ready, gate) = (ready_write.as_raw_fd(), gate_read.as_raw_fd());
            // SAFETY: the pre-exec hook performs only async-signal-safe pipe operations.
            unsafe {
                command.pre_exec(move || {
                    let pid = libc::getpid().to_ne_bytes();
                    if libc::write(ready, pid.as_ptr().cast(), pid.len()) != 4 {
                        return Err(io::Error::last_os_error());
                    }
                    let mut byte = 0_u8;
                    if libc::read(gate, (&raw mut byte).cast(), 1) != 1 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let mut group = thread::scope(|scope| {
                let spawned =
                    scope.spawn(|| command.spawn_locked().expect("spawn subtree fixture"));
                let mut bytes = [0_u8; 4];
                io::Read::read_exact(&mut &ready_read, &mut bytes).expect("held leader pid");
                let pid = i32::from_ne_bytes(bytes);
                let mut group = HeldSubtreeGroup { pid, leader: None };
                monitor
                    .watch(u32::try_from(pid).unwrap())
                    .expect("watch before release");
                gate_write.write_all(b"x").unwrap();
                group.leader = Some(spawned.join().unwrap());
                group
            });
            let leader = group.leader.as_mut().unwrap();
            let input = leader.stdin.take().unwrap();
            let mut lines = io::BufReader::new(leader.stdout.take().unwrap()).lines();
            let (mut root, mut child, mut sibling) = (None, None, None);
            for _ in 0..2 {
                let line = lines.next().unwrap().unwrap();
                let fields: Vec<_> = line.split_whitespace().collect();
                match fields.as_slice() {
                    ["root", root_pid, child_pid] => {
                        root = Some(native_life(root_pid.parse().unwrap()));
                        child = Some(native_life(child_pid.parse().unwrap()));
                    }
                    ["sibling", pid] => sibling = Some(native_life(pid.parse().unwrap())),
                    _ => panic!("unexpected subtree fixture report: {line}"),
                }
            }
            let fixture = Self {
                monitor,
                group,
                input,
                lines,
                root: root.unwrap(),
                child: child.unwrap(),
                sibling: sibling.unwrap(),
            };
            // Admission itself drains/discovers, so both ancestry edges are retained before a
            // test lets the root exit. There is no late ownership reconstruction from a pid.
            fixture.handle(fixture.root);
            fixture.handle(fixture.child);
            fixture
        }

        fn handle(&self, peer: ProcessIdentity) -> JobSubtreeHandle {
            match self.monitor.subtree_handle(peer).unwrap() {
                JobSubtreeAdmission::Admitted { handle } => handle,
                refused => panic!("an authentic fixture peer was refused: {refused:?}"),
            }
        }

        fn command(&mut self, command: &str, expected: &str) {
            use std::io::Write as _;
            writeln!(self.input, "{command}").unwrap();
            assert_eq!(self.lines.next().unwrap().unwrap(), expected);
        }

        fn retired(&self, handle: JobSubtreeHandle) -> JobProcessRetirement {
            match self
                .monitor
                .retire_subtree(handle, Duration::from_millis(20))
                .unwrap()
            {
                JobSubtreeRetirement::Retired { retirement } => retirement,
                refused => panic!("an issued subtree was refused: {refused:?}"),
            }
        }

        fn assert_ended(&self, life: ProcessIdentity, signal: i32) {
            use crate::api::dto::ExitStatus;
            assert!(
                !self.monitor.running_lives().contains(&life),
                "exit was not folded"
            );
            assert!(
                crate::runtime::job_groups::process_record(
                    libc::pid_t::try_from(life.pid).unwrap()
                )
                .unwrap()
                .is_none_or(|record| record.unique_id != life.birth.0),
                "life still runs"
            );
            let tree = self.monitor.tree().unwrap();
            let process = tree
                .processes
                .iter()
                .find(|process| process.pid == life.pid)
                .unwrap();
            assert_eq!(
                process.exit.as_ref().and_then(|exit| exit.status.as_ref()),
                Some(&ExitStatus::Signaled {
                    signal,
                    core_dumped: false
                })
            );
        }

        fn assert_siblings_live(&self) {
            assert_eq!(native_life(self.sibling.pid), self.sibling);
            let leader = u32::try_from(self.group.pid).unwrap();
            assert!(self.monitor.owns_live(native_life(leader)).unwrap());
            assert!(self.monitor.owns_live(self.sibling).unwrap());
        }
    }

    #[test]
    fn concurrent_subtree_retirements_preserve_siblings_while_they_fork_and_exec() {
        let fixtures: Vec<_> = (0..8)
            .map(|_| SubtreeFixture::start_with_churn(true))
            .collect();
        let ready = std::sync::Barrier::new(8);
        thread::scope(|scope| {
            let witnesses: Vec<_> = fixtures
                .into_iter()
                .map(|mut fixture| {
                    let ready = &ready;
                    scope.spawn(move || {
                        let handle = fixture.handle(fixture.root);
                        ready.wait();
                        fixture.command("c", "churning");
                        let receipt = fixture.retired(handle);
                        let mut actual: Vec<_> =
                            receipt.outlived.iter().map(|row| row.pid).collect();
                        actual.sort_unstable();
                        let mut expected = vec![fixture.root.pid, fixture.child.pid];
                        expected.sort_unstable();
                        assert_eq!(actual, expected, "{receipt:?}");
                        assert!(
                            receipt
                                .outlived
                                .iter()
                                .all(|row| row.signal == libc::SIGKILL)
                        );
                        assert!(!receipt.outlived_incomplete);
                        for life in [fixture.root, fixture.child] {
                            assert!(
                                !fixture.monitor.running_lives().contains(&life),
                                "exit not joined"
                            );
                        }
                        assert_eq!(fixture.lines.next().unwrap().unwrap(), "churned 64");
                        fixture.assert_siblings_live();
                    })
                })
                .collect();
            for witness in witnesses {
                witness.join().unwrap();
            }
        });
    }

    #[test]
    fn subtree_retirement_joins_a_reparented_child_after_its_root_exits() {
        let mut fixture = SubtreeFixture::start();
        let handle = fixture.handle(fixture.root);
        fixture.command("x", "exited");
        let child = crate::runtime::job_groups::process_record(
            libc::pid_t::try_from(fixture.child.pid).unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(child.unique_id, fixture.child.birth.0);
        assert_ne!(
            child.ppid, fixture.root.pid,
            "the child must really have reparented"
        );
        assert!(matches!(
            fixture.monitor.subtree_handle(fixture.root).unwrap(),
            JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::NotMember
            }
        ));
        fixture.monitor.gap(ProcessCoverageGap::EventsLost).unwrap();
        let receipt = fixture.retired(handle.clone());
        assert_eq!(receipt.outlived.len(), 1, "{receipt:?}");
        assert_eq!(
            (receipt.outlived[0].pid, receipt.outlived[0].signal),
            (fixture.child.pid, libc::SIGKILL)
        );
        assert!(!receipt.outlived_incomplete);
        assert!(
            matches!(&receipt.containment, JobContainment::Observed { lost }
            if lost.contains(&ProcessCoverageGap::EventsLost)),
            "{receipt:?}"
        );
        fixture.assert_ended(fixture.child, libc::SIGKILL);
        fixture.assert_siblings_live();
        assert_eq!(
            fixture
                .monitor
                .retire_subtree(handle, Duration::ZERO)
                .unwrap(),
            JobSubtreeRetirement::Refused {
                reason: JobSubtreeRefusal::SpentHandle
            }
        );
    }

    #[test]
    fn subtree_exec_preserves_handle_and_receipts_include_root_and_terminal_provenance() {
        let mut fixture = SubtreeFixture::start();
        let handle = fixture.handle(fixture.root);
        fixture.command("e", "exec");
        assert_eq!(native_life(fixture.root.pid), fixture.root);
        assert_eq!(
            fixture.handle(fixture.root),
            handle,
            "exec must not mint another capability"
        );
        let receipt = fixture.retired(handle);
        let mut signalled: Vec<_> = receipt.outlived.iter().map(|row| row.pid).collect();
        signalled.sort_unstable();
        let mut expected = vec![fixture.root.pid, fixture.child.pid];
        expected.sort_unstable();
        assert_eq!(signalled, expected, "{receipt:?}");
        for row in &receipt.outlived {
            assert!(row.index.is_some(), "{row:?}");
            assert_eq!(row.signal, libc::SIGKILL);
        }
        fixture.assert_ended(fixture.root, libc::SIGKILL);
        fixture.assert_ended(fixture.child, libc::SIGKILL);
        fixture.assert_siblings_live();
        let terminal = fixture
            .monitor
            .retire(
                Birth::of(u32::try_from(fixture.group.pid).unwrap()),
                Duration::ZERO,
            )
            .unwrap();
        for prior in receipt.outlived {
            assert!(
                terminal.outlived.contains(&prior),
                "whole-job retirement lost subtree provenance: {terminal:?}"
            );
        }
    }

    #[test]
    fn subtree_foreign_forged_stale_and_index_substituted_handles_cannot_signal() {
        let fixture = SubtreeFixture::start();
        let handle = fixture.handle(fixture.root);
        let sibling = fixture.handle(fixture.sibling);
        let mut forged = handle.clone();
        forged.incarnation = "not-an-issued-nonce".to_owned();
        let mut cross_job = handle.clone();
        cross_job.job_id = JobId::new(402).unwrap();
        let mut wrong_index = handle.clone();
        wrong_index.index = sibling.index;
        let mut unknown = handle.clone();
        unknown.index = u32::MAX;
        for foreign in [forged, cross_job, wrong_index, unknown] {
            assert_eq!(
                fixture
                    .monitor
                    .retire_subtree(foreign, Duration::ZERO)
                    .unwrap(),
                JobSubtreeRetirement::Refused {
                    reason: JobSubtreeRefusal::ForeignHandle
                }
            );
        }
        // A recreated monitor with the same public job id has no authority over the old nonce.
        let replacement = ProcessMonitor::prepare(TreeOwner::Job(handle.job_id)).unwrap();
        assert_eq!(
            replacement
                .retire_subtree(handle.clone(), Duration::ZERO)
                .unwrap(),
            JobSubtreeRetirement::Refused {
                reason: JobSubtreeRefusal::ForeignHandle
            }
        );
        let foreign = ProcessMonitor::prepare(TreeOwner::Job(JobId::new(402).unwrap())).unwrap();
        assert_eq!(
            foreign.retire_subtree(handle, Duration::ZERO).unwrap(),
            JobSubtreeRetirement::Refused {
                reason: JobSubtreeRefusal::ForeignHandle
            }
        );
        let stale = ProcessIdentity {
            birth: BirthToken(fixture.root.birth.0.wrapping_add(1)),
            ..fixture.root
        };
        assert!(!fixture.monitor.owns_live(stale).unwrap());
        assert!(matches!(
            fixture.monitor.subtree_handle(stale).unwrap(),
            JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::NotMember
            }
        ));
        assert!(
            !fixture
                .monitor
                .owns_live(native_life(std::process::id()))
                .unwrap()
        );
        assert_eq!(native_life(fixture.root.pid), fixture.root);
        assert_eq!(native_life(fixture.child.pid), fixture.child);
        fixture.assert_siblings_live();
    }

    #[test]
    fn subtree_first_use_after_natural_exit_is_empty_then_spent() {
        let mut fixture = SubtreeFixture::start();
        let handle = fixture.handle(fixture.root);
        fixture.command("n", "exited");
        let receipt = fixture.retired(handle.clone());
        assert!(receipt.outlived.is_empty(), "{receipt:?}");
        assert!(!receipt.outlived_incomplete);
        assert!(matches!(
            receipt.containment,
            JobContainment::Observed { .. }
        ));
        assert_eq!(
            fixture
                .monitor
                .retire_subtree(handle, Duration::ZERO)
                .unwrap(),
            JobSubtreeRetirement::Refused {
                reason: JobSubtreeRefusal::SpentHandle
            }
        );
        fixture.assert_siblings_live();
    }

    #[test]
    fn subtree_operational_failure_does_not_consume_the_capability() {
        let fixture = SubtreeFixture::start();
        let handle = fixture.handle(fixture.root);
        // The native TERM step succeeds, but an unrepresentable grace cannot open a deadline.
        // All target lives ignore TERM, and the pre-held guard exists before this can fail.
        assert!(
            fixture
                .monitor
                .retire_subtree(handle.clone(), Duration::MAX)
                .is_err()
        );
        assert_eq!(fixture.handle(fixture.root), handle);
        let receipt = fixture.retired(handle);
        assert_eq!(receipt.outlived.len(), 2, "{receipt:?}");
        fixture.assert_ended(fixture.root, libc::SIGKILL);
        fixture.assert_ended(fixture.child, libc::SIGKILL);
        fixture.assert_siblings_live();
    }

    /// A python leader of its own group, held before its exec until the monitor watches it,
    /// forks one descendant -- ignoring SIGTERM, in a new session, as asked -- which reports its
    /// pid, closes its stdio and waits for a signal. The leader exits on a byte of stdin and is
    /// left unreaped while the job retires, as the supervisor holds a leader; the descendant is
    /// joined before the proof is answered, and the proof is answered again unchanged.
    fn retires_a_descendant(ignore_term: bool, detach: bool, grace: Duration) {
        use std::io::{BufRead as _, Write as _};
        use std::os::unix::process::CommandExt as _;
        use std::process::{Command, Stdio};

        use cowshed_os::fork_lock::Spawn as _;

        use crate::api::dto::ExitStatus;
        use crate::runtime::job_groups::{await_exit_unreaped, process_record};

        let script = r#"
import os, signal, sys
if os.fork() == 0:
    if sys.argv[1] == "ignore":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if sys.argv[2] == "detach":
        os.setsid()
    os.write(1, b"%d\n" % os.getpid())
    for fd in (0, 1, 2):
        os.close(fd)
    while True:
        signal.pause()
os.read(0, 1)
os._exit(0)
"#;
        let monitor = ProcessMonitor::prepare(TreeOwner::Job(JobId::new(1).unwrap())).unwrap();
        let (ready_read, ready_write) = std::io::pipe().unwrap();
        let (gate_read, mut gate_write) = std::io::pipe().unwrap();
        let mut command = Command::new("/bin/bash");
        command
            .args([
                "-c",
                "exec python3 -c \"$1\" \"$2\" \"$3\"",
                "retire-fixture",
                script,
                if ignore_term { "ignore" } else { "honor" },
                if detach { "detach" } else { "group" },
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .process_group(0);
        let (ready, gate) = (ready_write.as_raw_fd(), gate_read.as_raw_fd());
        // SAFETY: the hook calls only async-signal-safe functions.
        unsafe {
            command.pre_exec(move || {
                let pid = libc::getpid().to_ne_bytes();
                if libc::write(ready, pid.as_ptr().cast(), pid.len()) != 4 {
                    return Err(io::Error::last_os_error());
                }
                let mut byte = 0_u8;
                if libc::read(gate, (&raw mut byte).cast(), 1) != 1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut leader = std::thread::scope(|scope| {
            let spawned = scope.spawn(|| command.spawn_locked().expect("spawn the leader"));
            let mut pid = [0_u8; 4];
            io::Read::read_exact(&mut &ready_read, &mut pid).expect("the leader's pid");
            let pid = u32::try_from(i32::from_ne_bytes(pid)).expect("pid");
            monitor.watch(pid).expect("watch the held leader");
            gate_write.write_all(b"x").expect("release the leader");
            spawned.join().expect("spawner")
        });
        let pid = leader.id();
        let birth = Birth::of(pid);
        assert!(birth.leader().is_some(), "{birth:?}");
        let mut line = String::new();
        io::BufReader::new(leader.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let descendant: u32 = line.trim().parse().expect("the descendant's pid");
        let life = process_record(i32::try_from(descendant).unwrap())
            .unwrap()
            .expect("the descendant runs");
        // Its parent's fork is folded while the parent still runs.
        while !monitor
            .running_lives()
            .iter()
            .any(|running| running.pid == descendant && running.birth == BirthToken(life.unique_id))
        {
            thread::sleep(Duration::from_millis(10));
        }
        leader.stdin.take().unwrap().write_all(b"x").unwrap();
        assert_eq!(
            await_exit_unreaped(i32::try_from(pid).unwrap()).unwrap(),
            ExitStatus::Exited { code: 0 }
        );

        let proof = monitor.retire(birth.clone(), grace).unwrap();
        let tree = monitor.tree().unwrap();
        let index = tree
            .processes
            .iter()
            .position(|process| process.pid == descendant)
            .expect("the descendant is retained");
        let signal = if ignore_term {
            libc::SIGKILL
        } else {
            libc::SIGTERM
        };
        assert_eq!(proof.outlived.len(), 1, "{proof:?}");
        let outlived = &proof.outlived[0];
        assert_eq!(
            (outlived.index, outlived.pid, outlived.signal),
            (u32::try_from(index).ok(), descendant, signal)
        );
        assert!(
            outlived
                .image
                .as_deref()
                .is_some_and(|image| image.contains("python")),
            "{outlived:?}"
        );
        assert!(
            matches!(proof.containment, JobContainment::Observed { .. }),
            "{proof:?}"
        );
        assert!(
            process_record(i32::try_from(descendant).unwrap())
                .unwrap()
                .is_none_or(|now| now.unique_id != life.unique_id),
            "the descendant still runs"
        );
        assert!(monitor.running_lives().is_empty());
        assert_eq!(
            tree.processes[index]
                .exit
                .as_ref()
                .and_then(|exit| exit.status.as_ref()),
            Some(&ExitStatus::Signaled {
                signal,
                core_dumped: false
            })
        );
        // The leader's exit was folded before anything was signalled: it gets no receipt.
        assert!(tree.processes[0].exit.is_some());
        assert_eq!(monitor.retire(birth, grace).unwrap(), proof);
        assert!(leader.wait_bounded().unwrap().success());
    }

    #[test]
    fn retirement_kills_and_joins_a_detached_descendant_that_ignores_term() {
        retires_a_descendant(true, true, Duration::from_millis(200));
    }

    #[test]
    fn retirement_terms_and_joins_a_descendant_in_the_held_group() {
        retires_a_descendant(false, false, Duration::from_secs(10));
    }

    /// Replay keeps every emitted row, in order, and a gap-only batch adds none. The writer gathers
    /// the batches of one seal interval into one segment (13_telemetry.md, "Process segments"), so
    /// the health read seals all three rows together.
    #[tokio::test]
    async fn trace_replay_preserves_emitted_rows_in_order_and_gap_only_batches_add_none() {
        let root = TempRoot::new("cowshed-process-batch-replay");
        let publisher = JobSpanPublisher::start(root.to_path_buf());
        let parent = ProcessIdentity {
            pid: 41,
            birth: BirthToken(1),
        };
        let birth = |pid, index, parent, batch_end| RetainedEntry {
            event: Ok(JobProcessEvent::Born {
                index,
                process: JobProcessSample {
                    pid,
                    ppid: if index == 0 { 1 } else { 41 },
                    program: "/bin/cat".into(),
                    argv: vec![CommandArg::from("/bin/cat")].into(),
                    truncated: index == 1,
                    born_at: UtcTimestamp::new("1970-01-01T00:00:01Z").unwrap(),
                    usage: None,
                    blocked_on: None,
                    exit: None,
                },
            }),
            identity: Some(ProcessIdentity {
                pid,
                birth: BirthToken(u64::from(index) + 1),
            }),
            parent,
            at: SystemTime::UNIX_EPOCH + Duration::from_secs(1),
            batch_end,
        };
        let journal = [
            birth(41, 0, None, false),
            birth(42, 1, Some(parent), true),
            birth(43, 2, Some(parent), true),
            RetainedEntry {
                event: Ok(JobProcessEvent::Gap {
                    reason: ProcessCoverageGap::EventsLost,
                }),
                identity: None,
                parent: None,
                at: SystemTime::UNIX_EPOCH + Duration::from_secs(2),
                batch_end: true,
            },
        ];
        let mut trace = TraceWriter {
            publisher: publisher.clone(),
            projection: ProcessSpanProjection::new("4bf92f3577b34da6a3ce929d0e0e4736", 9),
        };
        trace.replay(&journal);
        for entry in &journal {
            if let Ok(JobProcessEvent::Born { index, process }) = &entry.event {
                assert_eq!(process.truncated, *index == 1);
            }
        }
        let health = publisher.health().await.unwrap();
        assert_eq!((health.recorded, health.failed), (1, 0), "{health:?}");
        let paths = std::fs::read_dir(root.join("1970-01-01"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 1);
        let rows = read_process_segment(&paths[0]).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].columns.proc_pid, Some(41));
        assert_eq!(rows[1].columns.proc_pid, Some(42));
        assert_eq!(rows[2].columns.proc_pid, Some(43));
        assert_eq!(rows[1].parent_span_id, rows[0].span_id);
        assert_eq!(rows[2].parent_span_id, rows[0].span_id);
    }

    /// An ended observation says JobEnded only for a life it held. To any other peer it is no
    /// member, so the supervisor goes on to ask the observation that owns the peer, rather than
    /// refusing a live job's admission because an unrelated job's observation ended first.
    #[test]
    fn an_ended_observation_answers_job_ended_only_for_its_own_life() {
        use cowshed_os::fork_lock::Spawn as _;

        let monitor = ProcessMonitor::prepare(TreeOwner::Job(JobId::new(1).unwrap())).unwrap();
        let mut child = std::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .spawn_locked()
            .expect("spawn the observed life");
        let pid = child.id();
        monitor.watch(pid).unwrap();
        let own = native_life(pid);
        drop(child.stdin.take());
        assert!(child.wait_bounded().unwrap().success());
        monitor.finish().unwrap();

        assert_eq!(
            monitor.subtree_handle(own).unwrap(),
            JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::JobEnded,
            },
            "the ended job's own life"
        );
        assert_eq!(
            monitor
                .subtree_handle(native_life(std::process::id()))
                .unwrap(),
            JobSubtreeAdmission::Refused {
                reason: JobSubtreeRefusal::NotMember,
            },
            "a life of another job"
        );
    }

    #[tokio::test]
    async fn finishing_a_live_observation_neither_invents_loss_nor_ends_its_reader() {
        let monitor = ProcessMonitor::prepare(TreeOwner::Job(JobId::new(1).unwrap())).unwrap();
        monitor.watch(std::process::id()).unwrap();
        assert_eq!(monitor.tree().unwrap().coverage, ProcessCoverage::Complete);
        monitor.finish().unwrap();
        assert_eq!(monitor.tree().unwrap().coverage, ProcessCoverage::Complete);
        let mut events = monitor.subscribe(Duration::from_millis(20)).await.unwrap();
        assert!(
            matches!(events.next().await, Some(Ok(JobProcessEvent::Born { process, .. })) if process.pid == std::process::id())
        );
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        loop {
            match tokio::time::timeout_at(deadline, events.next()).await {
                Ok(Some(Ok(_))) => {}
                Ok(other) => panic!("the live owner's reader ended or failed: {other:?}"),
                Err(_) => break,
            }
        }
        drop(events);
        assert!(monitor.tree().unwrap().processes[0].exit.is_none());
    }

    /// The supervisor stamps the job's clock after the watch has read the root: that read opens
    /// no window, and the windows the writer's ticks close are placed on the clock it was told.
    #[tokio::test]
    async fn a_clocked_monitor_places_the_windows_its_ticks_close_on_the_job_s_clock() {
        use crate::api::resources::WallMicros;

        let monitor = ProcessMonitor::prepare(TreeOwner::Job(JobId::new(1).unwrap())).unwrap();
        monitor.watch(std::process::id()).unwrap();
        let origin = Instant::now();
        monitor.clock(origin).unwrap();
        // A subscriber shortens the writer's ticks after its first, and every usage
        // read those ticks make is an event on this subscription: the first closed
        // window arrives as one, never found by re-reading the tree on a clock.
        let mut events = monitor.subscribe(Duration::from_millis(20)).await.unwrap();
        let window = loop {
            use crate::api::process::JobProcessDelta;
            match events.next().await {
                Some(Ok(JobProcessEvent::Changed(JobProcessDelta::Usage { usage, .. }))) => {
                    if let Some(window) = usage.cpu_window {
                        break window;
                    }
                }
                Some(Ok(
                    JobProcessEvent::Born { process, .. }
                    | JobProcessEvent::Exec { process, .. }
                    | JobProcessEvent::Heartbeat { process, .. }
                    | JobProcessEvent::Exited { process, .. },
                )) => {
                    if let Some(window) = process.usage.and_then(|usage| usage.cpu_window) {
                        break window;
                    }
                }
                Some(Ok(_)) => {}
                other => panic!("the event stream ended before a window closed: {other:?}"),
            }
        };
        // The watch's own read came before the origin; the first window opened at a tick after it.
        assert!(window.opened_us > WallMicros::new(0).unwrap(), "{window:?}");
        assert!(window.opened_us < window.closed_us, "{window:?}");
        assert!(
            window.closed_us <= WallMicros::of(origin.elapsed()).unwrap(),
            "{window:?} is on a clock that started at the origin it was told"
        );
        monitor.finish().unwrap();
    }
}
